package mieru

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/InazumaV/V2bX/api/panel"
	"github.com/InazumaV/V2bX/conf"
	vCore "github.com/InazumaV/V2bX/core"

	mierucommon "github.com/enfein/mieru/v3/apis/common"
	mieruconstant "github.com/enfein/mieru/v3/apis/constant"
	mierumodel "github.com/enfein/mieru/v3/apis/model"
	mieruserver "github.com/enfein/mieru/v3/apis/server"
	mierupb "github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
	"google.golang.org/protobuf/proto"
)

var _ vCore.Core = (*Mieru)(nil)

type UserTraffic struct {
	Upload   atomic.Int64
	Download atomic.Int64
}

type Mieru struct {
	mu          sync.Mutex
	server      mieruserver.Server
	running     bool
	nodeInfo    *panel.NodeInfo
	config      *conf.Options
	trafficMap  sync.Map // uuid (string) -> *UserTraffic
	uidMap      sync.Map // uuid (string) -> int (userId)
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
		server:     mieruserver.NewServer(),
		trafficMap: sync.Map{},
		uidMap:     sync.Map{},
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

	config := &mieruserver.ServerConfig{
		Config: &mierupb.ServerConfig{
			PortBindings: portBindings,
			Users:        users,
		},
		StreamListenerFactory: MieruListenerFactory{},
		PacketListenerFactory: MieruListenerFactory{},
	}

	err := m.server.Store(config)
	if err != nil {
		return fmt.Errorf("failed to store config: %w", err)
	}

	if m.running && !m.server.IsRunning() && len(users) > 0 {
		if err := m.server.Start(); err != nil {
			return fmt.Errorf("failed to start server: %w", err)
		}
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
		// 没有用户时延迟启动，待 AddUsers 拉取用户后在 applyConfig 中自动触发热启动
		return nil
	}

	if err := m.server.Start(); err != nil {
		return err
	}

	go m.acceptLoop()
	return nil
}

func (m *Mieru) Close() error {
	m.mu.Lock()
	m.running = false
	m.mu.Unlock()

	if m.server.IsRunning() {
		return m.server.Stop()
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

		conn, req, err := m.server.Accept()
		if err != nil {
			if !running {
				break
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		go m.handleConnection(conn, req)
	}
}

func (m *Mieru) handleConnection(conn net.Conn, req *mierumodel.Request) {
	defer conn.Close()

	resp := &mierumodel.Response{
		Reply: mieruconstant.Socks5ReplySuccess,
		BindAddr: mierumodel.AddrSpec{
			IP:   net.IPv4zero,
			Port: 0,
		},
	}
	if err := resp.WriteToSocks5(conn); err != nil {
		return
	}

	var username string
	if uCtx, ok := conn.(mierucommon.UserContext); ok {
		username = uCtx.UserName()
	}
	if username == "" {
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
			return
		}
		destAddr := net.JoinHostPort(destHost, fmt.Sprintf("%d", req.DstAddr.Port))

		remoteConn, err := net.DialTimeout("tcp", destAddr, 10*time.Second)
		if err != nil {
			return
		}
		defer remoteConn.Close()

		m.bridgeTCP(conn, remoteConn, username)

	case mieruconstant.Socks5UDPAssociateCmd:
		m.bridgeUDP(conn, username)
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
		return
	}
	defer localUdpConn.Close()

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
