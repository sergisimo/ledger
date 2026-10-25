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
	ConnID string

	authPhase int

	Conn struct {
		id  ConnID
		ws  *websocket.Conn
		srv *Server
		log logging.Logger

		ctx    context.Context
		cancel context.CancelFunc
		done   chan struct{}

		send    chan Message
		closing chan closeFrame

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
	ErrConnClosed     = errors.New("websocket connection is closed")
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
		send:    make(chan Message, s.cfg.sendBuffer),
		closing: make(chan closeFrame, 1),
	}
	conn.log = s.log.With("connID", string(conn.id))

	conn.deadline = time.AfterFunc(s.cfg.authTimeout, conn.onDeadline)

	return conn
}

// --------------------------------------------------------------- Public API

func (c *Conn) ID() ConnID {
	return c.id
}

func (c *Conn) Context() context.Context {
	return c.ctx
}

func (c *Conn) Identity() authx.Identity {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.identity
}

func (c *Conn) Start() {
	defer close(c.done)
	defer c.deadline.Stop()

	written := make(chan struct{})
	go func() {
		defer close(written)
		c.writePump()
	}()

	c.readPump()
	<-written
}

func (c *Conn) Send(msg Message) error {
	if c.ctx.Err() != nil {
		return ErrConnClosed
	}

	select {
	case c.send <- msg:
		return nil
	default:
		return ErrSendBufferFull
	}
}

func (c *Conn) SendPayload(id, msgType string, v any) error {
	msg, err := NewMessage(id, msgType, v)
	if err != nil {
		return err
	}
	return c.Send(msg)
}

func (c *Conn) Close(code websocket.StatusCode, reason string) {
	select {
	case c.closing <- closeFrame{code: code, reason: reason}:
	default:
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

		case msg := <-c.send:
			if err := c.write(msg); err != nil {
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

func (c *Conn) write(msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		c.log.Error(c.ctx, "dropping unencodable %q message: %v", msg.Type, err)
		return nil
	}

	ctx, cancel := context.WithTimeout(c.ctx, c.srv.cfg.writeTimeout)
	defer cancel()

	return c.ws.Write(ctx, websocket.MessageText, data)
}

func (c *Conn) ping() error {
	ctx, cancel := context.WithTimeout(c.ctx, c.srv.cfg.pongTimeout)
	defer cancel()

	return c.ws.Ping(ctx)
}

func (c *Conn) closeWith(frame closeFrame) {
	for {
		select {
		case msg := <-c.send:
			if err := c.write(msg); err != nil {
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

	if err := c.srv.handler.Handle(c.ctx, c, msg); err != nil {
		c.log.Error(c.ctx, "handler failed for %q message: %v", msg.Type, err)
		c.reject(msg.ID, err.Error())
	}
}

func (c *Conn) reject(id, reason string) {
	if err := c.SendPayload(id, TypeError, ErrorPayload{Message: reason}); err != nil {
		c.log.Debug(c.ctx, "could not report %q to the client: %v", reason, err)
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
	c.identity = identity
	c.phase = phaseAuthenticated
	if identity.ExpiresAt.IsZero() {
		c.deadline.Stop()
	} else {
		c.deadline.Reset(time.Until(identity.ExpiresAt))
	}
	c.mu.Unlock()

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
