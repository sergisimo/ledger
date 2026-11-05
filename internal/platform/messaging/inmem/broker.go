// Package inmem provides an in-process message broker implementing the
// messaging contracts. It lets a monolith adopt event-driven design without
// infrastructure; scaling to a networked broker later is an fx wiring change.
//
// Delivery is asynchronous: each active subscription owns a buffered queue
// drained by one worker goroutine, preserving per-subscription FIFO order.
// Every subscription of a topic receives every message. Queued messages live
// only in memory and are lost on shutdown.
package inmem

import (
	"context"
	"errors"
	"sync"

	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/messaging"
)

type (
	// Broker is an in-memory messaging.MessagePublisher. It accepts publishes
	// and subscriptions between Start and Stop; subscriptions are built with
	// NewSubscriber. Most applications wire it with Module.
	Broker struct {
		log    logging.Logger
		stopCh chan struct{}

		mu    sync.Mutex
		state state
		subs  map[string]map[*Subscriber]struct{}
	}

	state int
)

const (
	stateIdle state = iota
	stateRunning
	stateStopped
)

var (
	// ErrBrokerNotRunning is returned by Publish and Subscribe outside the
	// Start/Stop window.
	ErrBrokerNotRunning = errors.New("in-memory broker is not running")
	// ErrBrokerClosed is returned by Start after Stop; a broker cannot be
	// restarted.
	ErrBrokerClosed = errors.New("in-memory broker is closed")
	// ErrMessageDropped is reported to a subscription's OnErrorFunc when its
	// queue is full and SubscriberWithOverflowDrop discards the message.
	ErrMessageDropped = errors.New("subscriber queue is full, message dropped")
)

// NewBroker builds a Broker that reports delivery problems to log. Call Start
// before publishing or subscribing.
func NewBroker(log logging.Logger) *Broker {
	return &Broker{
		log:    log,
		stopCh: make(chan struct{}),
		subs:   map[string]map[*Subscriber]struct{}{},
	}
}

// Start makes the broker accept publishes and subscriptions. It is a no-op on
// a running broker and returns ErrBrokerClosed on a stopped one.
func (b *Broker) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == stateStopped {
		return ErrBrokerClosed
	}
	b.state = stateRunning

	return nil
}

// Stop rejects further publishes, stops every active subscription waiting for
// in-flight handlers until ctx expires, and discards queued messages. It is
// idempotent.
func (b *Broker) Stop(ctx context.Context) error {
	b.mu.Lock()
	if b.state == stateStopped {
		b.mu.Unlock()
		return nil
	}
	b.state = stateStopped

	var subs []*Subscriber
	for _, set := range b.subs {
		for sub := range set {
			subs = append(subs, sub)
		}
	}
	b.subs = map[string]map[*Subscriber]struct{}{}
	b.mu.Unlock()

	close(b.stopCh)

	var errs []error
	for _, sub := range subs {
		if err := sub.stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// Publish enqueues each message for every subscription of its topic. With the
// default overflow policy it blocks on full queues until there is space or ctx
// expires. A message with an empty topic fails the whole batch before anything
// is enqueued.
func (b *Broker) Publish(ctx context.Context, msgs ...messaging.Message) error {
	for _, msg := range msgs {
		if err := msg.Validate(); err != nil {
			return err
		}
	}

	for _, msg := range msgs {
		if err := b.deliver(ctx, msg); err != nil {
			return err
		}
	}

	return nil
}

func (b *Broker) deliver(ctx context.Context, msg messaging.Message) error {
	b.mu.Lock()
	if b.state != stateRunning {
		b.mu.Unlock()
		return ErrBrokerNotRunning
	}
	subs := make([]*Subscriber, 0, len(b.subs[msg.Topic]))
	for sub := range b.subs[msg.Topic] {
		subs = append(subs, sub)
	}
	b.mu.Unlock()

	for _, sub := range subs {
		if err := sub.enqueue(ctx, msg); err != nil {
			return err
		}
	}

	return nil
}

func (b *Broker) attach(sub *Subscriber) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state != stateRunning {
		return ErrBrokerNotRunning
	}

	set := b.subs[sub.topic]
	if set == nil {
		set = map[*Subscriber]struct{}{}
		b.subs[sub.topic] = set
	}
	set[sub] = struct{}{}

	return nil
}

func (b *Broker) detach(sub *Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.subs[sub.topic], sub)
}
