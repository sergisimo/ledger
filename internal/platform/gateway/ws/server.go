package ws

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/coder/websocket"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/sergisimo/ledger/internal/platform/authx"
	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/logging"
)

// --------------------------------------------------------------- Contract

type (
	Middleware func(http.Handler) http.Handler

	serverCfg struct {
		address           string
		path              string
		readHeaderTimeout time.Duration
		tlsConfig         *tls.Config
		originPatterns    []string
		middlewares       []Middleware
		logger            logging.Logger

		authenticator  authx.Authenticator
		handler        Handler
		authTimeout    time.Duration
		refreshTimeout time.Duration
		pingInterval   time.Duration
		pongTimeout    time.Duration
		writeTimeout   time.Duration
		sendBuffer     int
		readLimit      int64
	}

	ServerOpt func(*serverCfg)
)

const (
	DefaultPath = "/ws"

	defaultReadHeaderTimeout = 10 * time.Second
	defaultAuthTimeout       = 10 * time.Second
	defaultRefreshTimeout    = 30 * time.Second
	defaultPingInterval      = 30 * time.Second
	defaultPongTimeout       = 10 * time.Second
	defaultWriteTimeout      = 10 * time.Second
	defaultSendBuffer        = 16
	defaultReadLimit         = 32 * 1024

	shuttingDownReason = "server is shutting down"
)

var ErrInvalidOriginPattern = errors.New("invalid origin pattern")

// --------------------------------------------------------------- Constructors

func ServerWithAddress(addr string) ServerOpt {
	return func(s *serverCfg) {
		s.address = addr
	}
}

func ServerWithPath(path string) ServerOpt {
	return func(s *serverCfg) {
		s.path = path
	}
}

func ServerWithReadHeaderTimeout(timeout time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.readHeaderTimeout = timeout
	}
}

func ServerWithTLSConfig(cfg *tls.Config) ServerOpt {
	return func(s *serverCfg) {
		s.tlsConfig = cfg
	}
}

func ServerWithOrigins(patterns ...string) ServerOpt {
	return func(s *serverCfg) {
		s.originPatterns = append(s.originPatterns, patterns...)
	}
}

func ServerWithMiddlewares(middlewares ...Middleware) ServerOpt {
	return func(s *serverCfg) {
		s.middlewares = append(s.middlewares, middlewares...)
	}
}

func ServerWithLogger(logger logging.Logger) ServerOpt {
	return func(s *serverCfg) {
		s.logger = logger
	}
}

func ServerWithAuthenticator(authenticator authx.Authenticator) ServerOpt {
	return func(s *serverCfg) {
		s.authenticator = authenticator
	}
}

func ServerWithHandler(handler Handler) ServerOpt {
	return func(s *serverCfg) {
		s.handler = handler
	}
}

func ServerWithAuthTimeout(timeout time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.authTimeout = timeout
	}
}

func ServerWithRefreshTimeout(timeout time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.refreshTimeout = timeout
	}
}

func ServerWithPingInterval(interval time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.pingInterval = interval
	}
}

func ServerWithPongTimeout(timeout time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.pongTimeout = timeout
	}
}

func ServerWithWriteTimeout(timeout time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.writeTimeout = timeout
	}
}

func ServerWithSendBuffer(size int) ServerOpt {
	return func(s *serverCfg) {
		s.sendBuffer = size
	}
}

func ServerWithReadLimit(limit int64) ServerOpt {
	return func(s *serverCfg) {
		s.readLimit = limit
	}
}

func defaultServerOpts() []ServerOpt {
	return []ServerOpt{
		ServerWithAddress(os.Getenv("WS_ADDRESS")),
		ServerWithPath(cmp.Or(os.Getenv("WS_PATH"), DefaultPath)),
		ServerWithReadHeaderTimeout(cmp.Or(parseDuration(os.Getenv("WS_READ_HEADER_TIMEOUT")), defaultReadHeaderTimeout)),
		ServerWithMiddlewares(otelMiddleware()),
		ServerWithAuthTimeout(cmp.Or(parseDuration(os.Getenv("WS_AUTH_TIMEOUT")), defaultAuthTimeout)),
		ServerWithRefreshTimeout(cmp.Or(parseDuration(os.Getenv("WS_REFRESH_TIMEOUT")), defaultRefreshTimeout)),
		ServerWithPingInterval(cmp.Or(parseDuration(os.Getenv("WS_PING_INTERVAL")), defaultPingInterval)),
		ServerWithPongTimeout(cmp.Or(parseDuration(os.Getenv("WS_PONG_TIMEOUT")), defaultPongTimeout)),
		ServerWithWriteTimeout(cmp.Or(parseDuration(os.Getenv("WS_WRITE_TIMEOUT")), defaultWriteTimeout)),
		ServerWithSendBuffer(defaultSendBuffer),
		ServerWithReadLimit(defaultReadLimit),
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
		return otelhttp.NewHandler(next, "ws.server")
	}
}

// --------------------------------------------------------------- Server

type Server struct {
	http    *http.Server
	accept  *websocket.AcceptOptions
	cfg     *serverCfg
	handler Handler
	log     logging.Logger

	mu    sync.Mutex
	conns map[ConnID]*Conn
}

func NewServer(opts ...ServerOpt) (*Server, error) {
	cfg := &serverCfg{
		middlewares: []Middleware{},
	}
	for _, opt := range append(defaultServerOpts(), opts...) {
		opt(cfg)
	}

	if cfg.logger == nil {
		return nil, fields.NewErrNil(fields.NameLogger)
	}

	if cfg.authenticator == nil {
		return nil, fields.NewErrNil(fields.NameAuthenticator)
	}

	if cfg.handler == nil {
		cfg.handler = loggingHandler(cfg.logger)
	}

	for _, pattern := range cfg.originPatterns {
		if _, err := filepath.Match(pattern, ""); err != nil {
			return nil, errors.Join(ErrInvalidOriginPattern, err)
		}
	}

	server := &Server{
		accept:  &websocket.AcceptOptions{OriginPatterns: cfg.originPatterns},
		cfg:     cfg,
		handler: cfg.handler,
		log:     cfg.logger,
		conns:   map[ConnID]*Conn{},
	}

	mux := http.NewServeMux()
	mux.Handle("GET "+cfg.path, wrapMiddlewares(http.HandlerFunc(server.handleUpgrade), cfg.middlewares...))

	server.http = &http.Server{
		Addr:              cfg.address,
		Handler:           mux,
		TLSConfig:         cfg.tlsConfig,
		ReadHeaderTimeout: cfg.readHeaderTimeout,
	}

	return server, nil
}

func (s *Server) Handler() http.Handler {
	return s.http.Handler
}

func (s *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

func (s *Server) Serve(listener net.Listener) error {
	if s.http.TLSConfig != nil {
		return s.http.ServeTLS(listener, "", "")
	}
	return s.http.Serve(listener)
}

func (s *Server) Shutdown(ctx context.Context) error {
	conns := s.beginShutdown()

	err := s.http.Shutdown(ctx)

	for _, conn := range conns {
		conn.Close(websocket.StatusGoingAway, shuttingDownReason)
	}

	for _, conn := range conns {
		select {
		case <-conn.done:
		case <-ctx.Done():
			s.closeNow(conns)
			return errors.Join(err, ctx.Err())
		}
	}

	return err
}

func (s *Server) Close() error {
	conns := s.beginShutdown()

	err := s.http.Close()
	s.closeNow(conns)

	return err
}

func (s *Server) beginShutdown() map[ConnID]*Conn {
	s.mu.Lock()
	defer s.mu.Unlock()

	conns := s.conns
	s.conns = nil

	return conns
}

func (s *Server) closeNow(conns map[ConnID]*Conn) {
	for _, conn := range conns {
		conn.cancel()
	}
	for _, conn := range conns {
		<-conn.done
	}
}

func (s *Server) shuttingDown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.conns == nil
}

func (s *Server) register(conn *Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conns == nil {
		return false
	}
	s.conns[conn.id] = conn

	return true
}

func (s *Server) unregister(conn *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.conns, conn.id)
}

func (s *Server) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	if s.shuttingDown() {
		http.Error(w, shuttingDownReason, http.StatusServiceUnavailable)
		return
	}

	wsConn, err := websocket.Accept(w, r, s.accept)
	if err != nil {
		s.log.Debug(r.Context(), "websocket upgrade rejected: %v", err)
		return
	}

	s.serveConn(r.Context(), wsConn)
}

func (s *Server) serveConn(ctx context.Context, wsConn *websocket.Conn) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn := s.newConn(ctx, cancel, wsConn)
	if !s.register(conn) {
		_ = wsConn.Close(websocket.StatusGoingAway, shuttingDownReason)
		return
	}
	defer s.unregister(conn)

	conn.Start()
}

func wrapMiddlewares(handler http.Handler, middlewares ...Middleware) http.Handler {
	current := handler
	for _, middleware := range slices.Backward(middlewares) {
		current = middleware(current)
	}
	return current
}
