package inmem

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"sync"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/messaging"
)

type (
	// Subscriber is a messaging.Subscriber bound to one topic and handler on a
	// Broker. Handler errors are logged (the broker cannot redeliver); dropped
	// messages and handler panics are reported to the OnErrorFunc given to
	// Subscribe. Handlers run on a context carrying the publishing trace.
	Subscriber struct {
		broker  *Broker
		topic   string
		handler messaging.HandlerFunc
		cfg     subscriberCfg

		mu sync.Mutex
		w  *worker
	}

	// SubscriberOpt configures a Subscriber. See the SubscriberWith functions.
	SubscriberOpt func(*subscriberCfg)

	subscriberCfg struct {
		buffer     int
		dropOnFull bool
	}

	worker struct {
		queue   chan messaging.Message
		stop    chan struct{}
		done    chan struct{}
		onError messaging.OnErrorFunc
	}
)

const defaultBuffer = 256

var droppedMessages = expvar.NewInt("messaging.inmem.dropped")

// SubscriberWithBuffer sets the subscription queue size; n < 1 keeps the
// default of 256.
func SubscriberWithBuffer(n int) SubscriberOpt {
	return func(cfg *subscriberCfg) {
		if n >= 1 {
			cfg.buffer = n
		}
	}
}

// SubscriberWithOverflowDrop drops messages when the queue is full, reporting
// each drop as an ErrMessageDropped to the OnErrorFunc, instead of blocking
// the publisher.
func SubscriberWithOverflowDrop() SubscriberOpt {
	return func(cfg *subscriberCfg) {
		cfg.dropOnFull = true
	}
}

// NewSubscriber builds a Subscriber delivering topic's messages to h on
// broker. Most applications wire it with messaging.SubscriberFx.
func NewSubscriber(broker *Broker, topic string, h messaging.HandlerFunc, opts ...SubscriberOpt) (*Subscriber, error) {
	if topic == "" {
		return nil, fields.NewErrInvalidEmptyString(messaging.FieldNameTopic)
	}
	if h == nil {
		return nil, fields.NewErrNil(messaging.FieldNameHandler)
	}

	cfg := subscriberCfg{buffer: defaultBuffer}
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Subscriber{broker: broker, topic: topic, handler: h, cfg: cfg}, nil
}

// Subscribe attaches the subscription to the broker and starts its worker.
func (s *Subscriber) Subscribe(_ context.Context, onError messaging.OnErrorFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.w != nil {
		return messaging.ErrAlreadySubscribed
	}

	if err := s.broker.attach(s); err != nil {
		return err
	}

	s.w = &worker{
		queue:   make(chan messaging.Message, s.cfg.buffer),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		onError: onError,
	}
	go s.run(s.w)

	return nil
}

// Unsubscribe detaches the subscription and stops its worker, waiting for an
// in-flight handler until ctx expires. Queued messages are discarded.
func (s *Subscriber) Unsubscribe(ctx context.Context) error {
	s.mu.Lock()
	w := s.w
	s.w = nil
	s.mu.Unlock()

	if w == nil {
		return messaging.ErrNotSubscribed
	}
	s.broker.detach(s)

	return stopWorker(ctx, w)
}

func (s *Subscriber) stop(ctx context.Context) error {
	s.mu.Lock()
	w := s.w
	s.w = nil
	s.mu.Unlock()

	if w == nil {
		return nil
	}

	return stopWorker(ctx, w)
}

func stopWorker(ctx context.Context, w *worker) error {
	close(w.stop)
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Subscriber) run(w *worker) {
	defer close(w.done)

	ctx := context.Background()
	for {
		select {
		case <-w.stop:
			return
		case msg := <-w.queue:
			s.consume(ctx, w, msg)
		}
	}
}

func (s *Subscriber) consume(ctx context.Context, w *worker, msg messaging.Message) {
	err := messaging.Consume(ctx, SystemName, msg, s.handler)
	switch {
	case err == nil:
	case errors.Is(err, messaging.ErrHandlerPanic):
		s.fail(ctx, w, err)
	default:
		s.broker.log.
			With("topic", s.topic).
			With("msgID", msg.ID).
			Error(ctx, "message handler failed: %v", err)
	}
}

func (s *Subscriber) enqueue(ctx context.Context, msg messaging.Message) error {
	s.mu.Lock()
	w := s.w
	s.mu.Unlock()

	if w == nil {
		return nil
	}

	if s.cfg.dropOnFull {
		select {
		case w.queue <- msg:
		default:
			droppedMessages.Add(1)
			s.fail(ctx, w, fmt.Errorf("message %s on topic %s: %w", msg.ID, s.topic, ErrMessageDropped))
		}
		return nil
	}

	select {
	case w.queue <- msg:
		return nil
	case <-w.stop:
		return nil
	case <-s.broker.stopCh:
		return ErrBrokerNotRunning
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Subscriber) fail(ctx context.Context, w *worker, err error) {
	if w.onError != nil {
		w.onError(ctx, err)
		return
	}

	s.broker.log.With("topic", s.topic).Error(ctx, "subscription failed: %v", err)
}
