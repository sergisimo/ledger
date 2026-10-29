package ws

import (
	"errors"
	"maps"
	"slices"
	"sync"
	"unicode"
	"unicode/utf8"
)

// --------------------------------------------------------------- Contract

type (
	subscriptions struct {
		mu     sync.RWMutex
		closed bool

		byTopic map[string]map[ConnID]*Conn
		byConn  map[ConnID]*connSubscriptions
	}

	connSubscriptions struct {
		conn   *Conn
		topics map[string]struct{}
	}
)

// ErrInvalidTopic reports an empty, too long or non-printable topic.
var ErrInvalidTopic = errors.New("invalid topic")

// --------------------------------------------------------------- Constructors

func newSubscriptions() *subscriptions {
	return &subscriptions{
		byTopic: map[string]map[ConnID]*Conn{},
		byConn:  map[ConnID]*connSubscriptions{},
	}
}

// --------------------------------------------------------------- Implementation

func (s *subscriptions) add(conn *Conn, topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrServerClosed
	}
	if conn.ctx.Err() != nil {
		return ErrConnClosed
	}

	entry, ok := s.byConn[conn.id]
	if !ok {
		entry = &connSubscriptions{conn: conn, topics: map[string]struct{}{}}
		s.byConn[conn.id] = entry
	}
	entry.topics[topic] = struct{}{}

	conns, ok := s.byTopic[topic]
	if !ok {
		conns = map[ConnID]*Conn{}
		s.byTopic[topic] = conns
	}
	conns[conn.id] = conn

	return nil
}

func (s *subscriptions) remove(connID ConnID, topic string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.byConn[connID]
	if !ok {
		return false
	}
	if _, ok := entry.topics[topic]; !ok {
		return false
	}

	delete(entry.topics, topic)
	if len(entry.topics) == 0 {
		delete(s.byConn, connID)
	}
	s.dropFromTopic(topic, connID)

	return true
}

func (s *subscriptions) removeConn(connID ConnID) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.byConn[connID]
	if !ok {
		return nil
	}
	delete(s.byConn, connID)

	topics := slices.Collect(maps.Keys(entry.topics))
	for _, topic := range topics {
		s.dropFromTopic(topic, connID)
	}
	slices.Sort(topics)

	return topics
}

func (s *subscriptions) dropFromTopic(topic string, connID ConnID) {
	conns, ok := s.byTopic[topic]
	if !ok {
		return
	}

	delete(conns, connID)
	if len(conns) == 0 {
		delete(s.byTopic, topic)
	}
}

func (s *subscriptions) snapshot(topic string) ([]*Conn, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrServerClosed
	}

	return slices.Collect(maps.Values(s.byTopic[topic])), nil
}

func (s *subscriptions) conn(connID ConnID) (*Conn, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.byConn[connID]
	if !ok {
		return nil, false
	}

	return entry.conn, true
}

func (s *subscriptions) has(connID ConnID, topic string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.byConn[connID]
	if !ok {
		return false
	}
	_, ok = entry.topics[topic]

	return ok
}

func (s *subscriptions) count(connID ConnID) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.byConn[connID]
	if !ok {
		return 0
	}

	return len(entry.topics)
}

func (s *subscriptions) topics(connID ConnID) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.byConn[connID]
	if !ok {
		return nil
	}

	topics := slices.Collect(maps.Keys(entry.topics))
	slices.Sort(topics)
	return topics
}

func (s *subscriptions) subscriberCount(topic string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.byTopic[topic])
}

func (s *subscriptions) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
}

// --------------------------------------------------------------- Validation

func validateTopic(topic string, maxLen int) error {
	if topic == "" || len(topic) > maxLen || !utf8.ValidString(topic) {
		return ErrInvalidTopic
	}

	for _, r := range topic {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return ErrInvalidTopic
		}
	}

	return nil
}
