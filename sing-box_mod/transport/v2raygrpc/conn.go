package v2raygrpc

import (
	"net"
	"os"
	"time"

	"github.com/sagernet/sing/common/baderror"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

var _ net.Conn = (*GRPCConn)(nil)

var grpcObfsKey = []byte("MOMclashGRPCObfuscationKey")

func xorInPlace(data []byte, key []byte) {
	for i := 0; i < len(data); i++ {
		data[i] ^= key[i%len(key)]
	}
}

type GRPCConn struct {
	GunService
	cache      []byte
	obfuscated bool
}

func NewGRPCConn(service GunService, obfuscated bool) *GRPCConn {
	//nolint:staticcheck
	if client, isClient := service.(GunService_TunClient); isClient {
		service = &clientConnWrapper{client}
	}
	return &GRPCConn{
		GunService: service,
		obfuscated: obfuscated,
	}
}

func (c *GRPCConn) Read(b []byte) (n int, err error) {
	if len(c.cache) > 0 {
		n = copy(b, c.cache)
		c.cache = c.cache[n:]
		return
	}
	hunk, err := c.Recv()
	err = baderror.WrapGRPC(err)
	if err != nil {
		return
	}
	if c.obfuscated && len(hunk.Data) > 0 {
		xorInPlace(hunk.Data, grpcObfsKey)
	}
	n = copy(b, hunk.Data)
	if n < len(hunk.Data) {
		c.cache = hunk.Data[n:]
	}
	return
}

func (c *GRPCConn) Write(b []byte) (n int, err error) {
	dataToSend := b
	if c.obfuscated {
		dataToSend = make([]byte, len(b))
		copy(dataToSend, b)
		xorInPlace(dataToSend, grpcObfsKey)
	}
	err = baderror.WrapGRPC(c.Send(&Hunk{Data: dataToSend}))
	if err != nil {
		return
	}
	return len(b), nil
}

func (c *GRPCConn) Close() error {
	return nil
}

func (c *GRPCConn) LocalAddr() net.Addr {
	return M.Socksaddr{}
}

func (c *GRPCConn) RemoteAddr() net.Addr {
	return M.Socksaddr{}
}

func (c *GRPCConn) SetDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *GRPCConn) SetReadDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *GRPCConn) SetWriteDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *GRPCConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *GRPCConn) Upstream() any {
	return c.GunService
}

var _ N.WriteCloser = (*clientConnWrapper)(nil)

type clientConnWrapper struct {
	GunService_TunClient
}

func (c *clientConnWrapper) CloseWrite() error {
	return c.CloseSend()
}
