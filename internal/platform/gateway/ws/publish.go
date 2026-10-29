package ws

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/coder/websocket"
)

// --------------------------------------------------------------- Contract

type (
	// Publisher delivers typed payloads to the subscribers of a topic.
	Publisher[T any] interface {
		Publish(ctx context.Context, topic string, payload T) (int, error)
	}

	publisher[T any] struct {
		server *Server
	}

	// OverflowPolicy decides what Publish does with a subscriber whose send buffer is full.
	OverflowPolicy int
)

const (
	// OverflowClose closes the subscriber with status 1008.
	OverflowClose OverflowPolicy = iota
	// OverflowDrop skips the subscriber for that message.
	OverflowDrop
)

const slowConsumerReason = "slow consumer"

// ErrServerClosed is returned by Publish after Shutdown or Close.
var ErrServerClosed = errors.New("websocket server is closed")

// --------------------------------------------------------------- Constructors

// NewPublisher returns a Publisher for payloads of type T.
func NewPublisher[T any](server *Server) Publisher[T] {
	return publisher[T]{server: server}
}

// --------------------------------------------------------------- Publishing

func (p publisher[T]) Publish(ctx context.Context, topic string, payload T) (int, error) {
	return p.server.Publish(ctx, topic, payload)
}

// Publish sends v to every subscriber of topic and returns how many were queued.
func (s *Server) Publish(ctx context.Context, topic string, v any) (int, error) {
	msg, err := NewTopicMessage(topic, v)
	if err != nil {
		return 0, err
	}

	frame, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}

	conns, err := s.subs.snapshot(topic)
	if err != nil {
		return 0, err
	}

	delivered := 0
	for _, conn := range conns {
		if err := conn.sendFrame(frame); err != nil {
			s.handleOverflow(ctx, conn, topic, err)
			continue
		}
		delivered++
	}

	return delivered, nil
}

// Unsubscribe revokes a subscription and reports whether it existed.
func (s *Server) Unsubscribe(connID ConnID, topic string) bool {
	conn, ok := s.subs.conn(connID)
	if !ok {
		return false
	}

	conn.subMu.Lock()
	defer conn.subMu.Unlock()

	if !s.subs.remove(connID, topic) {
		return false
	}
	s.onUnsubscribe(conn, topic, UnsubscribeByServer)

	return true
}

// SubscriberCount returns the number of connections subscribed to topic.
func (s *Server) SubscriberCount(topic string) int {
	return s.subs.subscriberCount(topic)
}

func (s *Server) handleOverflow(ctx context.Context, conn *Conn, topic string, err error) {
	if !errors.Is(err, ErrSendBufferFull) {
		return
	}

	log := conn.log.With("topic", topic)

	switch s.cfg.overflowPolicy {
	case OverflowDrop:
		log.Warn(ctx, "dropping message for slow consumer")

	default:
		log.Warn(ctx, "closing slow consumer")
		conn.Close(websocket.StatusPolicyViolation, slowConsumerReason)
	}
}

// --------------------------------------------------------------- Hooks

func (s *Server) onUnsubscribe(conn *Conn, topic string, reason UnsubscribeReason) {
	if s.cfg.subscriptionHandler == nil {
		return
	}

	s.cfg.subscriptionHandler.OnUnsubscribe(context.WithoutCancel(conn.ctx), conn, topic, reason)
}

func (s *Server) dropSubscriptions(conn *Conn, reason UnsubscribeReason) {
	conn.subMu.Lock()
	defer conn.subMu.Unlock()

	for _, topic := range s.subs.removeConn(conn.id) {
		s.onUnsubscribe(conn, topic, reason)
	}
}
