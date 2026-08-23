package rest

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// --------------------------------------------------------------- Contract

type (
	Controller interface {
		BasePath() string
		Endpoints() []*Endpoint
	}

	Middleware func(http.Handler) http.Handler

	serverCfg struct {
		readTimeout  time.Duration
		writeTimeout time.Duration
		idleTimeout  time.Duration
		address      string
		tlsConfig    *tls.Config

		controllers []Controller
		middlewares []Middleware
	}

	serverOpt func(*serverCfg)
)

// --------------------------------------------------------------- Constructors

func ServerWithReadTimeout(timeout time.Duration) serverOpt {
	return func(s *serverCfg) {
		s.readTimeout = timeout
	}
}

func ServerWithWriteTimeout(timeout time.Duration) serverOpt {
	return func(s *serverCfg) {
		s.writeTimeout = timeout
	}
}

func ServerWithIdleTimeout(timeout time.Duration) serverOpt {
	return func(s *serverCfg) {
		s.idleTimeout = timeout
	}
}

func ServerWithAddress(addr string) serverOpt {
	return func(s *serverCfg) {
		s.address = addr
	}
}

func ServerWithTLSConfig(cfg *tls.Config) serverOpt {
	return func(s *serverCfg) {
		s.tlsConfig = cfg
	}
}

func ServerWithControllers(controllers ...Controller) serverOpt {
	return func(s *serverCfg) {
		s.controllers = append(s.controllers, controllers...)
	}
}

func ServerWithMiddlewares(middlewares ...Middleware) serverOpt {
	return func(s *serverCfg) {
		s.middlewares = append(s.middlewares, middlewares...)
	}
}

func defaultServerOpts() []serverOpt {
	return []serverOpt{
		ServerWithReadTimeout(parseDuration(os.Getenv("REST_READ_TIMEOUT"))),
		ServerWithWriteTimeout(parseDuration(os.Getenv("REST_WRITE_TIMEOUT"))),
		ServerWithIdleTimeout(parseDuration(os.Getenv("REST_IDLE_TIMEOUT"))),
		ServerWithAddress(os.Getenv("REST_ADDRESS")),
		ServerWithMiddlewares(otelMiddleware()),
	}
}

func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

func otelMiddleware() Middleware {
	return func(next http.Handler) http.Handler {
		return otelhttp.NewHandler(next, "http.server")
	}
}

func NewServer(opts ...serverOpt) (*http.Server, error) {
	cfg := &serverCfg{
		controllers: []Controller{},
		middlewares: []Middleware{},
	}
	for _, opt := range append(defaultServerOpts(), opts...) {
		opt(cfg)
	}

	server := &http.Server{
		ReadTimeout:  cfg.readTimeout,
		WriteTimeout: cfg.writeTimeout,
		IdleTimeout:  cfg.idleTimeout,
		Addr:         cfg.address,
		TLSConfig:    cfg.tlsConfig,
	}

	mux := http.NewServeMux()
	for _, ctrl := range cfg.controllers {
		for _, ep := range ctrl.Endpoints() {
			mux.Handle(
				fmt.Sprintf("%s %s", ep.Method, path.Join(ctrl.BasePath(), ep.Path)),
				wrapMiddlewares(ep.Handler, cfg.middlewares...),
			)
		}
	}
	server.Handler = mux

	return server, nil
}

// wrapMiddlewares applies the given middlewares to the provided handler in the order they are given.
func wrapMiddlewares(handler http.Handler, middlewares ...Middleware) http.Handler {
	current := handler
	for i := len(middlewares) - 1; i >= 0; i-- {
		current = middlewares[i](current)
	}
	return current
}
