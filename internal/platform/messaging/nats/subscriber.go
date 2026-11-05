package nats

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/messaging"
)

type (
	// Subscriber is a messaging.Subscriber bound to one topic, consumer name
	// and handler. Each Subscribe declares the topology and pulls from the
	// durable consumer on the client's shared connection, processing
	// messages in order of delivery on one worker; see the package
	// documentation for the retry and dead-letter semantics. Handlers run on
	// a context carrying the publishing trace.
	Subscriber struct {
		client  *Client
		topic   string
		name    string
		handler messaging.HandlerFunc
		cfg     subscriberCfg

		mu sync.Mutex
		w  *worker
	}

	// SubscriberOpt configures a Subscriber. See the SubscriberWith functions.
	SubscriberOpt func(*subscriberCfg)

	subscriberCfg struct {
		maxAttempts     int
		initialBackoff  time.Duration
		maxBackoff      time.Duration
		ackWait         time.Duration
		deadLetterTopic string
	}

	worker struct {
		js      jetstream.JetStream
		iter    jetstream.MessagesContext
		cancel  context.CancelFunc
		done    chan struct{}
		onError messaging.OnErrorFunc
	}
)

const (
	defaultInitialBackoff = time.Second
	defaultMaxBackoff     = time.Minute
	defaultAckWait        = 30 * time.Second

	// prefetch is how many messages a subscription pulls ahead; Unsubscribe
	// drains them before stopping.
	prefetch = 16

	// settleTimeout bounds every dead-letter publish.
	settleTimeout = 5 * time.Second

	// nextRetryDelay spaces out pulls failing while the client reconnects.
	nextRetryDelay = time.Second
)

// SubscriberWithMaxAttempts dead-letters or terminates a message once it has
// been delivered n times without success; n < 1 keeps the default of
// redelivering forever.
func SubscriberWithMaxAttempts(n int) SubscriberOpt {
	return func(cfg *subscriberCfg) {
		if n >= 1 {
			cfg.maxAttempts = n
		}
	}
}

// SubscriberWithBackoff sets the redelivery delay after a failed attempt:
// initial after the first, doubling up to the cap. Non-positive values keep
// the defaults of 1s and 1m.
func SubscriberWithBackoff(initial, cap time.Duration) SubscriberOpt {
	return func(cfg *subscriberCfg) {
		if initial > 0 {
			cfg.initialBackoff = initial
		}
		if cap > 0 {
			cfg.maxBackoff = cap
		}
	}
}

// SubscriberWithAckWait sets how long the server waits for an
// acknowledgement before redelivering a message; keep it above the slowest
// handler. Non-positive values keep the default of 30s.
func SubscriberWithAckWait(d time.Duration) SubscriberOpt {
	return func(cfg *subscriberCfg) {
		if d > 0 {
			cfg.ackWait = d
		}
	}
}

// SubscriberWithDeadLetterTopic moves exhausted and Permanent messages to
// topic's stream instead of terminating them.
func SubscriberWithDeadLetterTopic(topic string) SubscriberOpt {
	return func(cfg *subscriberCfg) {
		cfg.deadLetterTopic = topic
	}
}

// NewSubscriber builds a Subscriber delivering topic's messages to h on
// client's connection, as the durable consumer named name. Most applications
// wire it with messaging.SubscriberFx, passing the consumer name as name.
func NewSubscriber(client *Client, topic, name string, h messaging.HandlerFunc, opts ...SubscriberOpt) (*Subscriber, error) {
	if client == nil {
		return nil, fields.NewErrNil(fields.NameClient)
	}
	if topic == "" {
		return nil, fields.NewErrInvalidEmptyString(messaging.FieldNameTopic)
	}
	if name == "" {
		return nil, fields.NewErrInvalidEmptyString(messaging.FieldNameConsumer)
	}
	if h == nil {
		return nil, fields.NewErrNil(messaging.FieldNameHandler)
	}

	cfg := subscriberCfg{
		initialBackoff: defaultInitialBackoff,
		maxBackoff:     defaultMaxBackoff,
		ackWait:        defaultAckWait,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.maxBackoff < cfg.initialBackoff {
		cfg.maxBackoff = cfg.initialBackoff
	}

	return &Subscriber{client: client, topic: topic, name: name, handler: h, cfg: cfg}, nil
}

// Durable returns the name of the durable consumer the subscription owns.
func (s *Subscriber) Durable() string {
	return streamName(s.name)
}

// Subscribe declares the stream and the durable consumer (plus the
// dead-letter stream when configured), connecting the client if needed, and
// starts the worker.
func (s *Subscriber) Subscribe(ctx context.Context, onError messaging.OnErrorFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.w != nil {
		return messaging.ErrAlreadySubscribed
	}

	js, err := s.client.jetstream(ctx)
	if err != nil {
		return err
	}

	stream, err := ensureStream(ctx, js, s.topic)
	if err != nil {
		return err
	}
	if s.cfg.deadLetterTopic != "" {
		if _, err := ensureStream(ctx, js, s.cfg.deadLetterTopic); err != nil {
			return err
		}
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       s.Durable(),
		FilterSubject: s.topic,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       s.cfg.ackWait,
		MaxDeliver:    -1,
	})
	if err != nil {
		return err
	}

	iter, err := consumer.Messages(jetstream.PullMaxMessages(prefetch))
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	w := &worker{js: js, iter: iter, cancel: cancel, done: make(chan struct{}), onError: onError}
	s.w = w
	go s.run(runCtx, w)

	return nil
}

// Unsubscribe stops pulling, lets the worker finish the messages it already
// fetched and waits for it until ctx expires; past the deadline it abandons
// the wait and cancels the worker, whose unacknowledged messages are
// redelivered after the ack wait.
func (s *Subscriber) Unsubscribe(ctx context.Context) error {
	s.mu.Lock()
	w := s.w
	s.w = nil
	s.mu.Unlock()

	if w == nil {
		return messaging.ErrNotSubscribed
	}

	w.iter.Drain()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		w.iter.Stop()
		w.cancel()
		return ctx.Err()
	}
}

func (s *Subscriber) run(ctx context.Context, w *worker) {
	defer close(w.done)
	defer w.cancel()

	for {
		msg, err := w.iter.Next()
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) || ctx.Err() != nil {
				return
			}
			s.fail(ctx, w, fmt.Errorf("pulling from consumer %s: %w", s.Durable(), err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(nextRetryDelay):
			}
			continue
		}

		s.consume(ctx, w, msg)
	}
}

// consume processes one delivery: it acknowledges on success, asks for a
// delayed redelivery on a transient failure and parks the message once it is
// permanent or exhausted. It leaves the message unacknowledged when the
// subscription is cancelled mid-handler, so the server redelivers it.
func (s *Subscriber) consume(ctx context.Context, w *worker, in jetstream.Msg) {
	msg := toMessage(in)
	delivered := 1
	if meta, err := in.Metadata(); err == nil && meta.NumDelivered > 0 {
		delivered = int(meta.NumDelivered)
	}

	attrs := []attribute.KeyValue{semconv.MessagingConsumerGroupName(s.Durable())}
	err := messaging.Consume(ctx, SystemName, msg, s.handler, attrs...)
	switch {
	case err == nil:
		s.settle(ctx, w, msg, in.Ack)
	case ctx.Err() != nil:
	case messaging.IsPermanent(err) || s.exhausted(delivered):
		s.park(ctx, w, in, msg, err)
	default:
		delay := s.backoff(delivered)
		s.log().With("msgID", msg.ID).With("attempts", delivered).Warn(ctx, "message handler failed, redelivering in %s: %v", delay, err)
		s.settle(ctx, w, msg, func() error { return in.NakWithDelay(delay) })
	}
}

// park moves a message the handler can never process to the dead-letter
// stream, or reports and terminates it when none is configured. The message
// is only acknowledged once parked, so a failed dead-letter publish leads to
// redelivery, not loss.
func (s *Subscriber) park(ctx context.Context, w *worker, in jetstream.Msg, msg messaging.Message, handlerErr error) {
	if s.cfg.deadLetterTopic == "" {
		s.fail(ctx, w, fmt.Errorf("message %s on topic %s terminated after a non-retryable failure: %w", msg.ID, msg.Topic, handlerErr))
		s.settle(ctx, w, msg, func() error { return in.TermWithReason(handlerErr.Error()) })
		return
	}

	dead := msg
	dead.Topic = s.cfg.deadLetterTopic
	dead.Headers = make(map[string]string, len(msg.Headers)+2)
	maps.Copy(dead.Headers, msg.Headers)
	dead.Headers[messaging.HeaderDeadLetterOrigin] = msg.Topic
	dead.Headers[messaging.HeaderDeadLetterReason] = handlerErr.Error()

	publishCtx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()
	if _, err := w.js.PublishMsg(publishCtx, toNatsMsg(dead)); err != nil {
		s.fail(ctx, w, fmt.Errorf("dead-lettering message %s from topic %s: %w", msg.ID, msg.Topic, err))
		s.settle(ctx, w, msg, func() error { return in.NakWithDelay(s.cfg.initialBackoff) })
		return
	}

	s.log().With("msgID", msg.ID).Warn(ctx, "message dead-lettered to %s: %v", s.cfg.deadLetterTopic, handlerErr)
	s.settle(ctx, w, msg, in.Ack)
}

// settle runs one acknowledgement call and reports failures: an
// unacknowledged message is redelivered after the ack wait.
func (s *Subscriber) settle(ctx context.Context, w *worker, msg messaging.Message, op func() error) {
	if err := op(); err != nil {
		s.fail(ctx, w, fmt.Errorf("settling message %s on topic %s: %w", msg.ID, msg.Topic, err))
	}
}

func (s *Subscriber) exhausted(delivered int) bool {
	return s.cfg.maxAttempts > 0 && delivered >= s.cfg.maxAttempts
}

func (s *Subscriber) backoff(attempts int) time.Duration {
	d := s.cfg.initialBackoff
	for i := 1; i < attempts; i++ {
		d *= 2
		if d <= 0 || d >= s.cfg.maxBackoff {
			return s.cfg.maxBackoff
		}
	}

	return min(d, s.cfg.maxBackoff)
}

func (s *Subscriber) fail(ctx context.Context, w *worker, err error) {
	if w.onError != nil {
		w.onError(ctx, err)
		return
	}

	s.log().Error(ctx, "subscription failed: %v", err)
}

func (s *Subscriber) log() logging.Logger {
	return s.client.log.With("topic", s.topic).With("consumer", s.name)
}
