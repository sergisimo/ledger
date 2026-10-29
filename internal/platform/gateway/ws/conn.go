package ws

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/sergisimo/ledger/internal/platform/authx"
	"github.com/sergisimo/ledger/internal/platform/logging"
)

// --------------------------------------------------------------- Contract

type (
	// ConnID identifies a connection for its lifetime.
	ConnID string

	authPhase int

	// Conn is an accepted connection. Methods are safe for concurrent use.
	Conn struct {
		id  ConnID
		ws  *websocket.Conn
		srv *Server
		log logging.Logger

		ctx    context.Context
		cancel context.CancelFunc
		done   chan struct{}

		send    chan []byte
		closing chan closeFrame

		subMu sync.Mutex

		mu       sync.Mutex
		phase    authPhase
		identity authx.Identity
		deadline *time.Timer
	}

	closeFrame struct {
		code   websocket.StatusCode
		reason string
	}
)

const (
	phaseUnauthenticated authPhase = iota
	phaseAuthenticated
	phaseRefreshing
)

var (
	// ErrConnClosed is returned by Send once the connection is closing.
	ErrConnClosed = errors.New("websocket connection is closed")
	// ErrSendBufferFull is returned by Send when the outgoing queue is full.
	ErrSendBufferFull = errors.New("websocket send buffer is full")
)

// --------------------------------------------------------------- Constructors

func (s *Server) newConn(ctx context.Context, cancel context.CancelFunc, wsConn *websocket.Conn) *Conn {
	wsConn.SetReadLimit(s.cfg.readLimit)

	conn := &Conn{
		id:      ConnID(uuid.NewString()),
		ws:      wsConn,
		srv:     s,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
		send:    make(chan []byte, s.cfg.sendBuffer),
		closing: make(chan closeFrame, 1),
	}
	conn.log = s.log.With("connID", string(conn.id))

	conn.deadline = time.AfterFunc(s.cfg.authTimeout, conn.onDeadline)

	return conn
}

// --------------------------------------------------------------- Public API

// ID returns the connection identifier.
func (c *Conn) ID() ConnID {
	return c.id
}

// Context is cancelled when the connection closes.
func (c *Conn) Context() context.Context {
	return c.ctx
}

// Identity returns the authenticated identity, zero before authentication.
func (c *Conn) Identity() authx.Identity {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.identity
}

// Subscriptions returns the subscribed topics, sorted.
func (c *Conn) Subscriptions() []string {
	return c.srv.subs.topics(c.id)
}

// Start runs the read and write loops and returns when the connection is done.
func (c *Conn) Start() {
	defer close(c.done)
	defer c.deadline.Stop()

	written := make(chan struct{})
	go func() {
		defer close(written)
		c.writePump()
	}()

	c.readPump()
	c.cancel()
	<-written
}

// Send queues msg without blocking.
func (c *Conn) Send(msg Message) error {
	frame, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return c.sendFrame(frame)
}

// SendPayload encodes v and sends it as a message of msgType.
func (c *Conn) SendPayload(id, msgType string, v any) error {
	msg, err := NewMessage(id, msgType, v)
	if err != nil {
		return err
	}
	return c.Send(msg)
}

// Close flushes the queue and closes the connection with code and reason.
func (c *Conn) Close(code websocket.StatusCode, reason string) {
	select {
	case c.closing <- closeFrame{code: code, reason: reason}:
	default:
	}
}

func (c *Conn) sendFrame(frame []byte) error {
	if c.ctx.Err() != nil {
		return ErrConnClosed
	}

	select {
	case c.send <- frame:
		return nil
	default:
		return ErrSendBufferFull
	}
}

// --------------------------------------------------------------- Pumps

func (c *Conn) readPump() {
	for {
		msgType, data, err := c.ws.Read(c.ctx)
		if err != nil {
			c.log.Debug(c.ctx, "stopped reading: %v", err)
			return
		}

		if msgType != websocket.MessageText {
			c.reject("", "only text messages are supported")
			continue
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			c.reject("", "malformed message")
			continue
		}

		c.dispatch(msg)
	}
}

func (c *Conn) writePump() {
	ticker := time.NewTicker(c.srv.cfg.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return

		case frame := <-c.closing:
			c.closeWith(frame)
			return

		case frame := <-c.send:
			if err := c.write(frame); err != nil {
				c.log.Debug(c.ctx, "stopped writing: %v", err)
				return
			}

		case <-ticker.C:
			if err := c.ping(); err != nil {
				c.log.Debug(c.ctx, "keepalive failed: %v", err)
				return
			}
		}
	}
}

func (c *Conn) write(frame []byte) error {
	ctx, cancel := context.WithTimeout(c.ctx, c.srv.cfg.writeTimeout)
	defer cancel()

	return c.ws.Write(ctx, websocket.MessageText, frame)
}

func (c *Conn) ping() error {
	ctx, cancel := context.WithTimeout(c.ctx, c.srv.cfg.pongTimeout)
	defer cancel()

	return c.ws.Ping(ctx)
}

func (c *Conn) closeWith(frame closeFrame) {
	for {
		select {
		case queued := <-c.send:
			if err := c.write(queued); err != nil {
				return
			}
			continue
		default:
		}
		break
	}

	_ = c.ws.Close(frame.code, frame.reason)
}

// --------------------------------------------------------------- Dispatch

func (c *Conn) dispatch(msg Message) {
	if msg.Type == TypeAuth {
		c.authenticate(msg)
		return
	}

	if c.currentPhase() != phaseAuthenticated {
		c.reject(msg.ID, "not authenticated")
		return
	}

	switch msg.Type {
	case TypeSubscribe:
		c.subscribe(msg)
	case TypeUnsubscribe:
		c.unsubscribe(msg)
	default:
		if err := c.srv.handler.Handle(c.ctx, c, msg); err != nil {
			c.log.Error(c.ctx, "handler failed for %q message: %v", msg.Type, err)
			c.reject(msg.ID, err.Error())
		}
	}
}

func (c *Conn) reject(id, reason string) {
	if err := c.SendPayload(id, TypeError, ErrorPayload{Message: reason}); err != nil {
		c.log.Debug(c.ctx, "could not report %q to the client: %v", reason, err)
	}
}

// --------------------------------------------------------------- Subscriptions

func (c *Conn) subscribe(msg Message) {
	topic, ok := c.bindTopic(msg)
	if !ok {
		return
	}

	handler := c.srv.cfg.subscriptionHandler
	if handler == nil {
		c.reject(msg.ID, "subscriptions are not enabled")
		return
	}

	c.subMu.Lock()
	defer c.subMu.Unlock()

	if c.srv.subs.has(c.id, topic) {
		c.confirm(msg.ID, TypeSubscribeOK, topic)
		return
	}

	if c.srv.subs.count(c.id) >= c.srv.cfg.maxSubscriptions {
		c.reject(msg.ID, "too many subscriptions")
		return
	}

	if err := handler.OnSubscribe(c.ctx, c, topic); err != nil {
		c.rejectSubscription(msg.ID, topic, err)
		return
	}

	if err := c.srv.subs.add(c, topic); err != nil {
		c.srv.onUnsubscribe(c, topic, UnsubscribeOnDisconnect)
		return
	}

	c.confirm(msg.ID, TypeSubscribeOK, topic)
}

func (c *Conn) unsubscribe(msg Message) {
	topic, ok := c.bindTopic(msg)
	if !ok {
		return
	}

	c.subMu.Lock()
	if c.srv.subs.remove(c.id, topic) {
		c.srv.onUnsubscribe(c, topic, UnsubscribeByClient)
	}
	c.subMu.Unlock()

	c.confirm(msg.ID, TypeUnsubscribeOK, topic)
}

func (c *Conn) bindTopic(msg Message) (string, bool) {
	var payload SubscribePayload
	if err := msg.Bind(&payload); err != nil {
		c.reject(msg.ID, "malformed subscription payload")
		return "", false
	}

	if err := validateTopic(payload.Topic, c.srv.cfg.maxTopicLength); err != nil {
		c.reject(msg.ID, "invalid topic")
		return "", false
	}

	return payload.Topic, true
}

func (c *Conn) rejectSubscription(id, topic string, err error) {
	switch {
	case errors.Is(err, ErrSubscriptionForbidden):
		c.reject(id, "forbidden")
	case errors.Is(err, ErrUnknownTopic):
		c.reject(id, "unknown topic")
	default:
		c.log.Error(c.ctx, "subscription handler failed for topic %q: %v", topic, err)
		c.reject(id, "subscription failed")
	}
}

func (c *Conn) confirm(id, msgType, topic string) {
	if err := c.SendPayload(id, msgType, SubscribePayload{Topic: topic}); err != nil {
		c.log.Debug(c.ctx, "could not confirm %q for topic %q: %v", msgType, topic, err)
	}
}

// --------------------------------------------------------------- Authentication

func (c *Conn) currentPhase() authPhase {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.phase
}

func (c *Conn) authenticate(msg Message) {
	var payload AuthPayload
	if err := msg.Bind(&payload); err != nil {
		c.reject(msg.ID, "malformed auth payload")
		return
	}

	identity, err := c.srv.cfg.authenticator.Authenticate(c.ctx, payload.Token)
	if err != nil {
		c.log.Warn(c.ctx, "rejected token: %v", err)
		c.reject(msg.ID, "authentication failed")
		c.Close(websocket.StatusPolicyViolation, "authentication failed")

		return
	}

	if identity.Expired() {
		c.reject(msg.ID, "token is already expired")
		c.Close(websocket.StatusPolicyViolation, "authentication failed")

		return
	}

	c.mu.Lock()
	previous := c.identity
	wasAuthenticated := c.phase != phaseUnauthenticated
	c.identity = identity
	c.phase = phaseAuthenticated
	if identity.ExpiresAt.IsZero() {
		c.deadline.Stop()
	} else {
		c.deadline.Reset(time.Until(identity.ExpiresAt))
	}
	c.mu.Unlock()

	if wasAuthenticated && previous.Subject != identity.Subject {
		c.log.Info(c.ctx, "subject changed on re-authentication, dropping subscriptions")
		c.srv.dropSubscriptions(c, UnsubscribeByServer)
	}

	expiresAt := &identity.ExpiresAt
	if identity.ExpiresAt.IsZero() {
		expiresAt = nil
	}
	if err := c.SendPayload(msg.ID, TypeAuthOK, AuthOKPayload{Subject: identity.Subject, ExpiresAt: expiresAt}); err != nil {
		c.log.Debug(c.ctx, "could not confirm authentication: %v", err)
	}
}

func (c *Conn) onDeadline() {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case phaseUnauthenticated:
		c.Close(websocket.StatusPolicyViolation, "authentication timeout")

	case phaseRefreshing:
		c.Close(websocket.StatusPolicyViolation, "token refresh timeout")

	case phaseAuthenticated:
		c.phase = phaseRefreshing
		c.deadline.Reset(c.srv.cfg.refreshTimeout)

		if err := c.SendPayload("", TypeAuthRefresh, nil); err != nil {
			c.log.Debug(c.ctx, "could not ask for a token refresh: %v", err)
		}
	}
}
