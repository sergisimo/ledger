package rpc

import (
	"context"
	"crypto/tls"
	"net"
	"os"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// --------------------------------------------------------------- Contract

type (
	Controller interface {
		Register(grpc.ServiceRegistrar)
	}

	serverCfg struct {
		address            string
		tlsConfig          *tls.Config
		unaryInterceptors  []grpc.UnaryServerInterceptor
		streamInterceptors []grpc.StreamServerInterceptor
		serverOptions      []grpc.ServerOption
		controllers        []Controller
		health             bool
		reflection         bool
	}

	ServerOpt func(*serverCfg)
)

// --------------------------------------------------------------- Constructors

func ServerWithAddress(addr string) ServerOpt {
	return func(s *serverCfg) {
		s.address = addr
	}
}

func ServerWithTLSConfig(cfg *tls.Config) ServerOpt {
	return func(s *serverCfg) {
		s.tlsConfig = cfg
	}
}

func ServerWithOptions(options ...grpc.ServerOption) ServerOpt {
	return func(s *serverCfg) {
		s.serverOptions = append(s.serverOptions, options...)
	}
}

func ServerWithControllers(controllers ...Controller) ServerOpt {
	return func(s *serverCfg) {
		s.controllers = append(s.controllers, controllers...)
	}
}

func ServerWithHealth(enabled bool) ServerOpt {
	return func(s *serverCfg) {
		s.health = enabled
	}
}

func ServerWithReflection(enabled bool) ServerOpt {
	return func(s *serverCfg) {
		s.reflection = enabled
	}
}

func defaultServerOpts() []ServerOpt {
	return []ServerOpt{
		ServerWithAddress(os.Getenv("GRPC_ADDRESS")),
		ServerWithHealth(true),
	}
}

// --------------------------------------------------------------- Server

type Server struct {
	*grpc.Server
	address string
}

func NewServer(opts ...ServerOpt) (*Server, error) {
	cfg := &serverCfg{
		controllers: []Controller{},
	}
	for _, opt := range append(defaultServerOpts(), opts...) {
		opt(cfg)
	}

	serverOptions := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(append([]grpc.UnaryServerInterceptor{errorMapperInterceptor()}, cfg.unaryInterceptors...)...),
		grpc.ChainStreamInterceptor(cfg.streamInterceptors...),
	}
	if cfg.tlsConfig != nil {
		serverOptions = append(serverOptions, grpc.Creds(credentials.NewTLS(cfg.tlsConfig)))
	}
	serverOptions = append(serverOptions, cfg.serverOptions...)

	server := grpc.NewServer(serverOptions...)

	for _, ctrl := range cfg.controllers {
		ctrl.Register(server)
	}

	if cfg.health {
		healthpb.RegisterHealthServer(server, health.NewServer())
	}
	if cfg.reflection {
		reflection.Register(server)
	}

	return &Server{Server: server, address: cfg.address}, nil
}

func (s *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

func errorMapperInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		res, err := handler(ctx, req)
		if err != nil {
			return res, ToStatusError(err)
		}
		return res, nil
	}
}
