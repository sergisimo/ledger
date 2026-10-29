package ws

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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
	// Middleware wraps the HTTP handler that performs the WebSocket upgrade.
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

		subscriptionHandler SubscriptionHandler
		maxSubscriptions    int
		maxTopicLength      int
		overflowPolicy      OverflowPolicy
	}

	// ServerOpt configures a Server. See the ServerWith functions.
	ServerOpt func(*serverCfg)
)

const (
	// DefaultPath is the upgrade path when WS_PATH is not set.
	DefaultPath = "/ws"

	defaultReadHeaderTimeout = 10 * time.Second
	defaultAuthTimeout       = 10 * time.Second
	defaultRefreshTimeout    = 30 * time.Second
	defaultPingInterval      = 30 * time.Second
	defaultPongTimeout       = 10 * time.Second
	defaultWriteTimeout      = 10 * time.Second
	defaultSendBuffer        = 16
	defaultReadLimit         = 32 * 1024
	defaultMaxSubscriptions  = 128
	defaultMaxTopicLength    = 256

	shuttingDownReason = "server is shutting down"
)

var (
	// ErrInvalidOriginPattern reports a malformed pattern given to ServerWithOrigins.
	ErrInvalidOriginPattern = errors.New("invalid origin pattern")
	// ErrInvalidServerOption reports an out-of-range value given to a ServerWith function.
	ErrInvalidServerOption = errors.New("invalid server option")
)

// --------------------------------------------------------------- Constructors

// ServerWithAddress sets the listen address. Defaults to WS_ADDRESS.
func ServerWithAddress(addr string) ServerOpt {
	return func(s *serverCfg) {
		s.address = addr
	}
}

// ServerWithPath sets the upgrade path. Defaults to WS_PATH or DefaultPath.
func ServerWithPath(path string) ServerOpt {
	return func(s *serverCfg) {
		s.path = path
	}
}

// ServerWithReadHeaderTimeout bounds the time to read request headers. Defaults to WS_READ_HEADER_TIMEOUT or 10s.
func ServerWithReadHeaderTimeout(timeout time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.readHeaderTimeout = timeout
	}
}

// ServerWithTLSConfig enables TLS.
func ServerWithTLSConfig(cfg *tls.Config) ServerOpt {
	return func(s *serverCfg) {
		s.tlsConfig = cfg
	}
}

// ServerWithOrigins allows cross-origin upgrades from hosts matching the patterns. Same-origin is always allowed.
func ServerWithOrigins(patterns ...string) ServerOpt {
	return func(s *serverCfg) {
		s.originPatterns = append(s.originPatterns, patterns...)
	}
}

// ServerWithMiddlewares wraps the upgrade handler, outermost first. OpenTelemetry is always applied.
func ServerWithMiddlewares(middlewares ...Middleware) ServerOpt {
	return func(s *serverCfg) {
		s.middlewares = append(s.middlewares, middlewares...)
	}
}

// ServerWithLogger sets the logger. Mandatory.
func ServerWithLogger(logger logging.Logger) ServerOpt {
	return func(s *serverCfg) {
		s.logger = logger
	}
}

// ServerWithAuthenticator sets the token authenticator. Mandatory.
func ServerWithAuthenticator(authenticator authx.Authenticator) ServerOpt {
	return func(s *serverCfg) {
		s.authenticator = authenticator
	}
}

// ServerWithHandler sets the handler for application messages. Defaults to logging them.
func ServerWithHandler(handler Handler) ServerOpt {
	return func(s *serverCfg) {
		s.handler = handler
	}
}

// ServerWithAuthTimeout bounds the time a client has to authenticate. Defaults to WS_AUTH_TIMEOUT or 10s.
func ServerWithAuthTimeout(timeout time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.authTimeout = timeout
	}
}

// ServerWithRefreshTimeout bounds the time a client has to answer an auth.refresh. Defaults to WS_REFRESH_TIMEOUT or 30s.
func ServerWithRefreshTimeout(timeout time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.refreshTimeout = timeout
	}
}

// ServerWithPingInterval sets the keepalive period. Defaults to WS_PING_INTERVAL or 30s.
func ServerWithPingInterval(interval time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.pingInterval = interval
	}
}

// ServerWithPongTimeout bounds the wait for a pong. Defaults to WS_PONG_TIMEOUT or 10s.
func ServerWithPongTimeout(timeout time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.pongTimeout = timeout
	}
}

// ServerWithWriteTimeout bounds each write. Defaults to WS_WRITE_TIMEOUT or 10s.
func ServerWithWriteTimeout(timeout time.Duration) ServerOpt {
	return func(s *serverCfg) {
		s.writeTimeout = timeout
	}
}

// ServerWithSendBuffer sets the per-connection outgoing queue size. Defaults to 16.
func ServerWithSendBuffer(size int) ServerOpt {
	return func(s *serverCfg) {
		s.sendBuffer = size
	}
}

// ServerWithReadLimit caps incoming message size in bytes. Defaults to 32 KiB.
func ServerWithReadLimit(limit int64) ServerOpt {
	return func(s *serverCfg) {
		s.readLimit = limit
	}
}

// ServerWithSubscriptionHandler enables subscriptions. Without it subscribe requests are refused.
func ServerWithSubscriptionHandler(handler SubscriptionHandler) ServerOpt {
	return func(s *serverCfg) {
		s.subscriptionHandler = handler
	}
}

// ServerWithMaxSubscriptions caps topics per connection. Defaults to WS_MAX_SUBSCRIPTIONS or 128.
func ServerWithMaxSubscriptions(limit int) ServerOpt {
	return func(s *serverCfg) {
		s.maxSubscriptions = limit
	}
}

// ServerWithMaxTopicLength caps topic length in bytes. Defaults to WS_MAX_TOPIC_LENGTH or 256.
func ServerWithMaxTopicLength(length int) ServerOpt {
	return func(s *serverCfg) {
		s.maxTopicLength = length
	}
}

// ServerWithOverflowPolicy sets how Publish treats a full send buffer. Defaults to OverflowClose.
func ServerWithOverflowPolicy(policy OverflowPolicy) ServerOpt {
	return func(s *serverCfg) {
		s.overflowPolicy = policy
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
		ServerWithMaxSubscriptions(cmp.Or(parseInt(os.Getenv("WS_MAX_SUBSCRIPTIONS")), defaultMaxSubscriptions)),
		ServerWithMaxTopicLength(cmp.Or(parseInt(os.Getenv("WS_MAX_TOPIC_LENGTH")), defaultMaxTopicLength)),
		ServerWithOverflowPolicy(OverflowClose),
	}
}

func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

func parseInt(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

func otelMiddleware() Middleware {
	return func(next http.Handler) http.Handler {
		return otelhttp.NewHandler(next, "ws.server")
	}
}

// --------------------------------------------------------------- Server

// Server accepts WebSocket connections, authenticates them and routes messages.
type Server struct {
	http    *http.Server
	accept  *websocket.AcceptOptions
	cfg     *serverCfg
	handler Handler
	log     logging.Logger

	mu    sync.Mutex
	conns map[ConnID]*Conn
	subs  *subscriptions
}

// NewServer builds a Server from the defaults, the environment and opts.
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

	if cfg.maxSubscriptions <= 0 {
		return nil, fmt.Errorf("%w: max subscriptions must be positive, got %d", ErrInvalidServerOption, cfg.maxSubscriptions)
	}
	if cfg.maxTopicLength <= 0 {
		return nil, fmt.Errorf("%w: max topic length must be positive, got %d", ErrInvalidServerOption, cfg.maxTopicLength)
	}
	if cfg.overflowPolicy != OverflowClose && cfg.overflowPolicy != OverflowDrop {
		return nil, fmt.Errorf("%w: unknown overflow policy %d", ErrInvalidServerOption, cfg.overflowPolicy)
	}

	server := &Server{
		accept:  &websocket.AcceptOptions{OriginPatterns: cfg.originPatterns},
		cfg:     cfg,
		handler: cfg.handler,
		log:     cfg.logger,
		conns:   map[ConnID]*Conn{},
		subs:    newSubscriptions(),
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

// Handler returns the HTTP handler, for mounting on an existing server.
func (s *Server) Handler() http.Handler {
	return s.http.Handler
}

// ListenAndServe listens on the configured address and serves until closed.
func (s *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

// Serve accepts connections on listener until closed.
func (s *Server) Serve(listener net.Listener) error {
	if s.http.TLSConfig != nil {
		return s.http.ServeTLS(listener, "", "")
	}
	return s.http.Serve(listener)
}

// Shutdown stops listening, closes clients with status 1001 and waits for them until ctx expires.
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

// Close stops listening and drops every connection immediately.
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
	s.subs.close()

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
	delete(s.conns, conn.id)
	s.mu.Unlock()

	s.dropSubscriptions(conn, UnsubscribeOnDisconnect)
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
