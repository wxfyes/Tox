package tls

import (
	"context"
	"io"
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

type SniffConn struct {
	net.Conn
	peeked []byte
}

func (c *SniffConn) Read(b []byte) (n int, err error) {
	if len(c.peeked) > 0 {
		n = copy(b, c.peeked)
		c.peeked = c.peeked[n:]
		return n, nil
	}
	return c.Conn.Read(b)
}

func (c *SniffConn) CloseWrite() error {
	if tcpConn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return tcpConn.CloseWrite()
	}
	return nil
}

func (c *SniffConn) CloseRead() error {
	if tcpConn, ok := c.Conn.(interface{ CloseRead() error }); ok {
		return tcpConn.CloseRead()
	}
	return nil
}

func ServerHandshake(ctx context.Context, conn net.Conn, config ServerConfig) (Conn, error) {
	// Sniff the first 5 bytes to detect plain HTTP or random scans
	var peeked [5]byte
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := io.ReadFull(conn, peeked[:])
	conn.SetReadDeadline(time.Time{}) // Clear deadline

	if err != nil {
		return nil, err
	}

	// TLS ClientHello always starts with 0x16 0x03 (TLS Handshake, TLS Version 3.x)
	isTLS := n >= 2 && peeked[0] == 0x16 && peeked[1] == 0x03

	if !isTLS {
		// Non-TLS probe (plain HTTP or random scanner). Mimic Nginx 400 Bad Request
		dateStr := time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
		response := "HTTP/1.1 400 Bad Request\r\n" +
			"Server: nginx\r\n" +
			"Date: " + dateStr + "\r\n" +
			"Content-Type: text/html\r\n" +
			"Content-Length: 150\r\n" +
			"Connection: close\r\n\r\n" +
			"<html>\r\n" +
			"<head><title>400 Bad Request</title></head>\r\n" +
			"<body>\r\n" +
			"<center><h1>400 Bad Request</h1></center>\r\n" +
			"<hr><center>nginx</center>\r\n" +
			"</body>\r\n" +
			"</html>\r\n"
		_, _ = conn.Write([]byte(response))
		_ = conn.Close()
		return nil, os.ErrInvalid
	}

	sniffConn := &SniffConn{
		Conn:   conn,
		peeked: peeked[:n],
	}

	ctx, cancel := context.WithTimeout(ctx, C.TCPTimeout)
	defer cancel()
	tlsConn, err := aTLS.ServerHandshake(ctx, sniffConn, config)
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
