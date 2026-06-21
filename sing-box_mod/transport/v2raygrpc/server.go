package v2raygrpc

import (
	"context"
	"net"
	"os"
	"strings"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	gM "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var _ adapter.V2RayServerTransport = (*Server)(nil)

type Server struct {
	ctx     context.Context
	logger  logger.ContextLogger
	handler adapter.V2RayServerTransportHandler
	server  *grpc.Server
	options option.V2RayGRPCOptions
}

func NewServer(ctx context.Context, logger logger.ContextLogger, options option.V2RayGRPCOptions, tlsConfig tls.ServerConfig, handler adapter.V2RayServerTransportHandler) (*Server, error) {
	var serverOptions []grpc.ServerOption
	if tlsConfig != nil {
		if !common.Contains(tlsConfig.NextProtos(), http2.NextProtoTLS) {
			tlsConfig.SetNextProtos(append([]string{"h2"}, tlsConfig.NextProtos()...))
		}
		serverOptions = append(serverOptions, grpc.Creds(NewTLSTransportCredentials(tlsConfig)))
	}
	if options.IdleTimeout > 0 {
		serverOptions = append(serverOptions, grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    time.Duration(options.IdleTimeout),
			Timeout: time.Duration(options.PingTimeout),
		}))
	}
	server := &Server{ctx, logger, handler, grpc.NewServer(serverOptions...), options}
	RegisterGunServiceCustomNameServer(server.server, server, options.ServiceName)
	return server, nil
}

func (s *Server) Tun(server GunService_TunServer) error {
	if len(s.options.Headers) > 0 {
		if grpcMetadata, loaded := gM.FromIncomingContext(server.Context()); loaded {
			for expectedKey, expectedListable := range s.options.Headers {
				lowerKey := strings.ToLower(expectedKey)
				clientValues := grpcMetadata.Get(lowerKey)
				matched := false
				for _, expVal := range expectedListable {
					for _, cliVal := range clientValues {
						if cliVal == expVal {
							matched = true
							break
						}
					}
					if matched {
						break
					}
				}
				if !matched {
					s.logger.Warn("gRPC access denied: request missing or incorrect header: " + expectedKey)
					return status.Error(codes.NotFound, "Not Found")
				}
			}
		} else {
			s.logger.Warn("gRPC access denied: request missing metadata")
			return status.Error(codes.NotFound, "Not Found")
		}
	}
	conn := NewGRPCConn(server, s.options.Obfuscated)
	var source M.Socksaddr
	if remotePeer, loaded := peer.FromContext(server.Context()); loaded {
		source = M.SocksaddrFromNet(remotePeer.Addr)
	}
	if grpcMetadata, loaded := gM.FromIncomingContext(server.Context()); loaded {
		forwardFrom := strings.Join(grpcMetadata.Get("X-Forwarded-For"), ",")
		if forwardFrom != "" {
			for _, from := range strings.Split(forwardFrom, ",") {
				originAddr := M.ParseSocksaddr(from)
				if originAddr.IsValid() {
					source = originAddr.Unwrap()
				}
			}
		}
	}
	done := make(chan struct{})
	go s.handler.NewConnectionEx(log.ContextWithNewID(s.ctx), conn, source, M.Socksaddr{}, N.OnceClose(func(it error) {
		close(done)
	}))
	<-done
	return nil
}

func (s *Server) mustEmbedUnimplementedGunServiceServer() {
}

func (s *Server) Network() []string {
	return []string{N.NetworkTCP}
}

func (s *Server) Serve(listener net.Listener) error {
	return s.server.Serve(listener)
}

func (s *Server) ServePacket(listener net.PacketConn) error {
	return os.ErrInvalid
}

func (s *Server) Close() error {
	s.server.Stop()
	return nil
}
