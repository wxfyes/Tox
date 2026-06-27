package tls

import (
	"context"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/sagernet/sing-box/common/badtls"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	aTLS "github.com/sagernet/sing/common/tls"
)

type ServerOptions struct {
	Context        context.Context
	Logger         log.ContextLogger
	Options        option.InboundTLSOptions
	KTLSCompatible bool
}

func NewServer(ctx context.Context, logger log.ContextLogger, options option.InboundTLSOptions) (ServerConfig, error) {
	return NewServerWithOptions(ServerOptions{
		Context: ctx,
		Logger:  logger,
		Options: options,
	})
}

func NewServerWithOptions(options ServerOptions) (ServerConfig, error) {
	if !options.Options.Enabled {
		return nil, nil
	}
	if !options.KTLSCompatible {
		if options.Options.KernelTx {
			options.Logger.Warn("enabling kTLS TX in current scenarios will definitely reduce performance, please checkout https://sing-box.sagernet.org/configuration/shared/tls/#kernel_tx")
		}
	}
	if options.Options.KernelRx {
		options.Logger.Warn("enabling kTLS RX will definitely reduce performance, please checkout https://sing-box.sagernet.org/configuration/shared/tls/#kernel_rx")
	}
	if options.Options.Reality != nil && options.Options.Reality.Enabled {
		return NewRealityServer(options.Context, options.Logger, options.Options)
	}
	return NewSTDServer(options.Context, options.Logger, options.Options)
}

func ServerHandshake(ctx context.Context, conn net.Conn, config ServerConfig) (Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, C.TCPTimeout)
	defer cancel()
	fragConn := &FragmentConn{Conn: conn}
	tlsConn, err := aTLS.ServerHandshake(ctx, fragConn, config)
	if err != nil {
		return nil, err
	}
	readWaitConn, err := badtls.NewReadWaitConn(tlsConn)
	if err == nil {
		return readWaitConn, nil
	} else if err != os.ErrInvalid {
		return nil, err
	}
	return tlsConn, nil
}

type FragmentConn struct {
	net.Conn
	state int // 0: init, 1: bypass
}

func (c *FragmentConn) Write(b []byte) (int, error) {
	if c.state == 1 {
		return c.Conn.Write(b)
	}
	c.state = 1

	// Check if this is a TLS Handshake message
	if len(b) > 300 && b[0] == 0x16 && b[1] == 0x03 {
		// 20% chance to skip fragmentation (mimic normal traffic)
		if rand.Intn(5) == 0 {
			return c.Conn.Write(b)
		}

		splitPos := 200 + rand.Intn(200)
		if splitPos >= len(b) {
			splitPos = len(b) / 2
		}

		n1, err := c.Conn.Write(b[:splitPos])
		if err != nil {
			return n1, err
		}

		// Random delay 1-5ms to prevent TCP coalescing
		time.Sleep(time.Duration(1+rand.Intn(4)) * time.Millisecond)

		n2, err := c.Conn.Write(b[splitPos:])
		if err != nil {
			return n1 + n2, err
		}
		return n1 + n2, nil
	}

	return c.Conn.Write(b)
}

func (c *FragmentConn) CloseWrite() error {
	if tcpConn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return tcpConn.CloseWrite()
	}
	return nil
}

func (c *FragmentConn) CloseRead() error {
	if tcpConn, ok := c.Conn.(interface{ CloseRead() error }); ok {
		return tcpConn.CloseRead()
	}
	return nil
}
