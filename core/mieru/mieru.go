package mieru

import (
	"context"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/common/format"
	"github.com/InazumaV/V2bX/common/rate"
	"github.com/InazumaV/V2bX/conf"
	vCore "github.com/InazumaV/V2bX/core"
	"github.com/InazumaV/V2bX/limiter"

	mierucommon "github.com/enfein/mieru/v3/apis/common"
	mieruconstant "github.com/enfein/mieru/v3/apis/constant"
	mierumodel "github.com/enfein/mieru/v3/apis/model"
	mierupb "github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
	"google.golang.org/protobuf/proto"

	mieruappcommon "github.com/enfein/mieru/v3/pkg/appctl/appctlcommon"
	mierucommonpkg "github.com/enfein/mieru/v3/pkg/common"
	mieruprotocol "github.com/enfein/mieru/v3/pkg/protocol"
	mierutrafficpattern "github.com/enfein/mieru/v3/apis/trafficpattern"
	"github.com/juju/ratelimit"
)

var _ vCore.Core = (*Mieru)(nil)

type UserTraffic struct {
	Upload   atomic.Int64
	Download atomic.Int64
}

type connSet struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func (s *connSet) Add(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[c] = struct{}{}
}

func (s *connSet) Remove(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, c)
}

func (s *connSet) CloseAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.conns = make(map[net.Conn]struct{})
}

type Mieru struct {
	mu          sync.Mutex
	mux         *mieruprotocol.Mux
	running     bool
	started     bool
	nodeInfo    *panel.NodeInfo
	config      *conf.Options
	trafficMap  sync.Map // uuid (string) -> *UserTraffic
	uidMap      sync.Map // uuid (string) -> int (userId)
	userConns   sync.Map // uuid (string) -> *connSet
	tag         string
}

type MieruListenerFactory struct{}

func (MieruListenerFactory) Listen(ctx context.Context, network, address string) (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(ctx, network, address)
}

func (MieruListenerFactory) ListenPacket(ctx context.Context, network, address string) (net.PacketConn, error) {
	var lc net.ListenConfig
	return lc.ListenPacket(ctx, network, address)
}

func init() {
	vCore.RegisterCore("mieru", New)
}

func New(c *conf.CoreConfig) (vCore.Core, error) {
	return &Mieru{
		mux:        mieruprotocol.NewMux(false),
		trafficMap: sync.Map{},
		uidMap:     sync.Map{},
		userConns:  sync.Map{},
	}, nil
}

func (m *Mieru) applyConfig() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.nodeInfo == nil || m.nodeInfo.Mieru == nil {
		return nil
	}

	transport := "TCP"
	if m.nodeInfo.Mieru.Transport != "" {
		transport = m.nodeInfo.Mieru.Transport
	}

	var transportProtocol *mierupb.TransportProtocol
	switch transport {
	case "TCP":
		transportProtocol = mierupb.TransportProtocol_TCP.Enum()
	case "UDP", "udp":
		transportProtocol = mierupb.TransportProtocol_UDP.Enum()
	default:
		transportProtocol = mierupb.TransportProtocol_TCP.Enum()
	}

	var portBindings []*mierupb.PortBinding
	portRangeStr := m.nodeInfo.Mieru.PortRange
	if portRangeStr != "" {
		portBindings = append(portBindings, &mierupb.PortBinding{
			PortRange: proto.String(portRangeStr),
			Protocol:  transportProtocol,
		})
	} else {
		port := m.nodeInfo.Mieru.CommonNode.ServerPort
		if port <= 0 {
			port = 443
		}
		portBindings = append(portBindings, &mierupb.PortBinding{
			Port:     proto.Int32(int32(port)),
			Protocol: transportProtocol,
		})
	}

	var users []*mierupb.User
	m.uidMap.Range(func(key, value any) bool {
		uuid := key.(string)
		users = append(users, &mierupb.User{
			Name:     proto.String(uuid),
			Password: proto.String(uuid),
		})
		return true
	})

	if len(users) == 0 {
		if m.started {
			stdlog.Printf("[Mieru] no users remaining, closing mux listener")
			_ = m.mux.Close()
			m.started = false
			m.mux = mieruprotocol.NewMux(false)
		}
		return nil
	}

	// 1. 设置用户
	m.mux.SetServerUsers(mieruappcommon.UserListToMap(users))
	stdlog.Printf("[Mieru] updated server users count: %d", len(users))

	// 2. 设置端点
	mtu := mierucommonpkg.DefaultMTU
	endpoints, err := mieruappcommon.PortBindingsToUnderlayProperties(portBindings, mtu)
	if err != nil {
		stdlog.Printf("[Mieru] failed to parse port bindings: %v", err)
		return fmt.Errorf("failed to get underlay properties: %w", err)
	}
	m.mux.SetEndpoints(endpoints)

	// 3. 设置 traffic pattern
	trafficPattern, err := mierutrafficpattern.NewConfig(nil)
	if err != nil {
		stdlog.Printf("[Mieru] failed to create traffic pattern config: %v", err)
		return fmt.Errorf("failed to new traffic pattern config: %w", err)
	}
	m.mux.SetTrafficPattern(trafficPattern)
	m.mux.SetServerUserHintIsMandatory(false)
	m.mux.SetStreamListenerFactory(MieruListenerFactory{})
	m.mux.SetPacketListenerFactory(MieruListenerFactory{})

	// 4. 检查是否需要启动
	if m.running && !m.started && len(users) > 0 {
		stdlog.Printf("[Mieru] starting mux on endpoints: %v", portBindings)
		if err := m.mux.Start(); err != nil {
			stdlog.Printf("[Mieru] failed to start mux on port bindings: %v", err)
			return fmt.Errorf("failed to start mux: %w", err)
		}
		m.started = true
		stdlog.Printf("[Mieru] mux started successfully, launching accept loop")
		go m.acceptLoop()
	}

	return nil
}

func (m *Mieru) Start() error {
	m.mu.Lock()
	m.running = true
	m.mu.Unlock()

	hasUsers := false
	m.uidMap.Range(func(key, value any) bool {
		hasUsers = true
		return false
	})

	if !hasUsers {
		stdlog.Printf("[Mieru] no users configured, delaying mux start until users are added")
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		stdlog.Printf("[Mieru] starting mux in Start()...")
		if err := m.mux.Start(); err != nil {
			stdlog.Printf("[Mieru] failed to start mux in Start(): %v", err)
			return err
		}
		m.started = true
		stdlog.Printf("[Mieru] mux started successfully, launching accept loop")
		go m.acceptLoop()
	}
	return nil
}

func (m *Mieru) Close() error {
	m.mu.Lock()
	m.running = false
	started := m.started
	m.started = false
	m.mu.Unlock()

	if started && m.mux != nil {
		stdlog.Printf("[Mieru] closing mux listener because node is removed or stopped")
		err := m.mux.Close()
		m.mux = mieruprotocol.NewMux(false)
		return err
	}
	return nil
}

func (m *Mieru) acceptLoop() {
	for {
		m.mu.Lock()
		running := m.running
		m.mu.Unlock()
		if !running {
			break
		}

		conn, req, err := m.accept()
		if err != nil {
			m.mu.Lock()
			running = m.running
			m.mu.Unlock()
			if !running {
				break
			}
			// 🛡️ 静默处理握手或连接异常，防止 GFW 狂暴探测/重放攻击产生日志暴雨导致 VPS 内存溢出或 I/O 假死
			// stdlog.Printf("[Mieru] accept or handshake error: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		go m.handleConnection(conn, req)
	}
}

func (m *Mieru) accept() (net.Conn, *mierumodel.Request, error) {
	m.mu.Lock()
	mux := m.mux
	m.mu.Unlock()
	if mux == nil {
		return nil, nil, fmt.Errorf("mux is nil")
	}

	conn, err := mux.Accept()
	if err != nil {
		return nil, nil, err
	}
	if _, ok := conn.(mierucommon.UserContext); !ok {
		conn.Close()
		return nil, nil, fmt.Errorf("connection doesn't implement UserContext interface")
	}

	mierucommonpkg.SetReadTimeout(conn, 10*time.Second)
	defer func() {
		mierucommonpkg.SetReadTimeout(conn, 0)
	}()
	req := &mierumodel.Request{}
	if err := req.ReadFromSocks5(conn); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, req, nil
}

func (m *Mieru) handleConnection(conn net.Conn, req *mierumodel.Request) {
	defer conn.Close()

	var username string
	if uCtx, ok := conn.(mierucommon.UserContext); ok {
		username = uCtx.UserName()
	}
	if username == "" {
		stdlog.Printf("[Mieru] connection doesn't have a valid username context")
		return
	}

	switch req.Command {
	case mieruconstant.Socks5ConnectCmd:
		var destHost string
		if req.DstAddr.FQDN != "" {
			destHost = req.DstAddr.FQDN
		} else if req.DstAddr.IP != nil {
			destHost = req.DstAddr.IP.String()
		} else {
			stdlog.Printf("[Mieru] connection destination address is invalid")
			return
		}
		destAddr := net.JoinHostPort(destHost, fmt.Sprintf("%d", req.DstAddr.Port))

		// Check limiter for device online report, speed limit and device limit
		limiterObj, err := limiter.GetLimiter(m.tag)
		var limitBucket *ratelimit.Bucket
		if err == nil {
			clientIP, _, splitErr := net.SplitHostPort(conn.RemoteAddr().String())
			if splitErr != nil {
				clientIP = conn.RemoteAddr().String()
			}
			taguuid := format.UserTag(m.tag, username)
			bucket, reject := limiterObj.CheckLimit(taguuid, clientIP, true, true)
			if reject {
				stdlog.Printf("[Mieru] connection rejected by limiter (device limit or speed limit) for user %s from ip %s", username, clientIP)
				return
			}
			limitBucket = bucket
		}

		remoteConn, err := net.DialTimeout("tcp", destAddr, 10*time.Second)
		if err != nil {
			// Auto fallback to tcp4 (IPv4 only) dial if double-stack dial fails
			remoteConn, err = net.DialTimeout("tcp4", destAddr, 10*time.Second)
			if err != nil {
				stdlog.Printf("[Mieru] failed to dial destination %s: %v", destAddr, err)
				return
			}
		}
		defer remoteConn.Close()

		resp := &mierumodel.Response{
			Reply: mieruconstant.Socks5ReplySuccess,
			BindAddr: mierumodel.AddrSpec{
				IP:   net.IPv4zero,
				Port: 0,
			},
		}
		if err := resp.WriteToSocks5(conn); err != nil {
			stdlog.Printf("[Mieru] failed to write Socks5 response: %v", err)
			return
		}

		var clientConn net.Conn = conn
		if limitBucket != nil {
			clientConn = rate.NewConnRateLimiter(conn, limitBucket)
		}

		m.bridgeTCP(clientConn, remoteConn, username)

	case mieruconstant.Socks5UDPAssociateCmd:
		// Check limiter for device online report and device limit
		limiterObj, err := limiter.GetLimiter(m.tag)
		if err == nil {
			clientIP, _, splitErr := net.SplitHostPort(conn.RemoteAddr().String())
			if splitErr != nil {
				clientIP = conn.RemoteAddr().String()
			}
			taguuid := format.UserTag(m.tag, username)
			_, reject := limiterObj.CheckLimit(taguuid, clientIP, false, true)
			if reject {
				stdlog.Printf("[Mieru] UDP connection rejected by limiter for user %s from ip %s", username, clientIP)
				return
			}
		}

		resp := &mierumodel.Response{
			Reply: mieruconstant.Socks5ReplySuccess,
			BindAddr: mierumodel.AddrSpec{
				IP:   net.IPv4zero,
				Port: 0,
			},
		}
		if err := resp.WriteToSocks5(conn); err != nil {
			stdlog.Printf("[Mieru] failed to write Socks5 response: %v", err)
			return
		}

		m.bridgeUDP(conn, username)
	default:
		stdlog.Printf("[Mieru] unsupported command type: %d", req.Command)
	}
}

type countedWriter struct {
	writer  io.Writer
	counter *atomic.Int64
}

func (w *countedWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n > 0 {
		w.counter.Add(int64(n))
	}
	return n, err
}

func (m *Mieru) bridgeTCP(client, remote net.Conn, username string) {
	counterVal, _ := m.trafficMap.LoadOrStore(username, &UserTraffic{})
	uCounter := counterVal.(*UserTraffic)

	csVal, _ := m.userConns.LoadOrStore(username, &connSet{conns: make(map[net.Conn]struct{})})
	cs := csVal.(*connSet)
	cs.Add(client)
	cs.Add(remote)
	defer func() {
		cs.Remove(client)
		cs.Remove(remote)
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		writer := &countedWriter{writer: remote, counter: &uCounter.Upload}
		_, _ = io.Copy(writer, client)
		_ = remote.Close()
	}()

	go func() {
		defer wg.Done()
		writer := &countedWriter{writer: client, counter: &uCounter.Download}
		_, _ = io.Copy(writer, remote)
		_ = client.Close()
	}()

	wg.Wait()
}

func parseSocks5UDP(b []byte) (string, []byte, error) {
	if len(b) < 6 {
		return "", nil, errors.New("packet too short")
	}
	atyp := b[3]
	var offset int
	var host string

	switch atyp {
	case 1:
		if len(b) < 10 {
			return "", nil, errors.New("IPv4 packet too short")
		}
		ip := net.IP(b[4:8])
		host = ip.String()
		offset = 8
	case 3:
		domainLen := int(b[4])
		if len(b) < 5+domainLen+2 {
			return "", nil, errors.New("Domain packet too short")
		}
		host = string(b[5 : 5+domainLen])
		offset = 5 + domainLen
	case 4:
		if len(b) < 22 {
			return "", nil, errors.New("IPv6 packet too short")
		}
		ip := net.IP(b[4:20])
		host = ip.String()
		offset = 20
	default:
		return "", nil, fmt.Errorf("unsupported ATYP: %d", atyp)
	}

	port := (int(b[offset]) << 8) | int(b[offset+1])
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	payload := b[offset+2:]
	return addr, payload, nil
}

func buildSocks5UDP(addr string, payload []byte) ([]byte, error) {
	hostStr, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}

	var atyp byte
	var addrBytes []byte
	ip := net.ParseIP(hostStr)
	if ip != nil {
		if ip.To4() != nil {
			atyp = 1
			addrBytes = ip.To4()
		} else {
			atyp = 4
			addrBytes = ip.To16()
		}
	} else {
		atyp = 3
		addrBytes = append([]byte{byte(len(hostStr))}, []byte(hostStr)...)
	}

	buf := make([]byte, 3+1+len(addrBytes)+2+len(payload))
	buf[3] = atyp
	copy(buf[4:], addrBytes)
	offset := 4 + len(addrBytes)
	buf[offset] = byte(port >> 8)
	buf[offset+1] = byte(port)
	copy(buf[offset+2:], payload)

	return buf, nil
}

func (m *Mieru) bridgeUDP(conn net.Conn, username string) {
	pc := mierucommon.NewPacketOverStreamTunnel(conn)
	defer pc.Close()

	localUdpConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		stdlog.Printf("[Mieru] bridgeUDP: failed to listen local UDP packet conn: %v", err)
		return
	}
	defer localUdpConn.Close()

	csVal, _ := m.userConns.LoadOrStore(username, &connSet{conns: make(map[net.Conn]struct{})})
	cs := csVal.(*connSet)
	cs.Add(conn)
	defer func() {
		cs.Remove(conn)
	}()

	counterVal, _ := m.trafficMap.LoadOrStore(username, &UserTraffic{})
	uCounter := counterVal.(*UserTraffic)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 65535)
		for {
			n, rAddr, err := localUdpConn.ReadFrom(buf)
			if err != nil {
				break
			}
			if n > 0 {
				uCounter.Download.Add(int64(n))
				respBuf, err := buildSocks5UDP(rAddr.String(), buf[:n])
				if err == nil {
					_, _ = pc.WriteTo(respBuf, rAddr)
				}
			}
		}
	}()

	buf := make([]byte, 65535)
	for {
		n, err := pc.Read(buf)
		if err != nil {
			break
		}
		if n > 0 {
			destAddr, payload, err := parseSocks5UDP(buf[:n])
			if err != nil {
				continue
			}

			uCounter.Upload.Add(int64(len(payload)))

			rAddr, err := net.ResolveUDPAddr("udp", destAddr)
			if err != nil {
				continue
			}

			_, _ = localUdpConn.WriteTo(payload, rAddr)
		}
	}

	_ = localUdpConn.Close()
	wg.Wait()
}

func (m *Mieru) AddNode(tag string, info *panel.NodeInfo, config *conf.Options) error {
	m.mu.Lock()
	m.nodeInfo = info
	m.config = config
	m.tag = tag
	m.running = true
	m.mu.Unlock()

	return m.applyConfig()
}

func (m *Mieru) DelNode(tag string) error {
	return m.Close()
}

func (m *Mieru) AddUsers(p *vCore.AddUsersParams) (added int, err error) {
	for _, user := range p.Users {
		m.uidMap.Store(user.Uuid, user.Id)
	}
	err = m.applyConfig()
	if err != nil {
		return 0, err
	}
	return len(p.Users), nil
}

func (m *Mieru) DelUsers(users []panel.UserInfo, tag string, info *panel.NodeInfo) error {
	for _, user := range users {
		m.uidMap.Delete(user.Uuid)
		m.trafficMap.Delete(user.Uuid)
		if val, ok := m.userConns.LoadAndDelete(user.Uuid); ok {
			stdlog.Printf("[Mieru] user %s is deleted (over-traffic), active connections will be terminated", user.Uuid)
			cs := val.(*connSet)
			cs.CloseAll()
		}
	}
	return m.applyConfig()
}

func (m *Mieru) GetUserTrafficSlice(tag string, reset bool) ([]panel.UserTraffic, error) {
	var trafficSlice []panel.UserTraffic
	m.trafficMap.Range(func(key, value any) bool {
		uuid := key.(string)
		counter := value.(*UserTraffic)

		up := counter.Upload.Load()
		down := counter.Download.Load()

		if up+down > 0 {
			if reset {
				counter.Upload.Store(0)
				counter.Download.Store(0)
			}

			userIdVal, ok := m.uidMap.Load(uuid)
			if ok {
				userId := userIdVal.(int)
				trafficSlice = append(trafficSlice, panel.UserTraffic{
					UID:      userId,
					Upload:   up,
					Download: down,
				})
			}
		}
		return true
	})

	if len(trafficSlice) == 0 {
		return nil, nil
	}
	return trafficSlice, nil
}

func (m *Mieru) Protocols() []string {
	return []string{"mieru"}
}

func (m *Mieru) Type() string {
	return "mieru"
}
