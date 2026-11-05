package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/messaging"
)

type (
	// Subscriber is a messaging.Subscriber bound to one topic, consumer name
	// and handler. Each Subscribe declares the topology and attaches a
	// consumer to the client's shared connection, processing the queue's
	// messages in order on one worker; see the package documentation for the
	// retry and dead-letter semantics. Handlers run on a context carrying the
	// publishing trace.
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
		deadLetterTopic string
	}

	worker struct {
		consumer *rabbitmqamqp.Consumer
		dlt      *rabbitmqamqp.Publisher
		cancel   context.CancelFunc
		done     chan struct{}
		onError  messaging.OnErrorFunc
	}
)

const (
	defaultInitialBackoff = time.Second
	defaultMaxBackoff     = time.Minute

	// settleTimeout bounds every settlement call, so a message handled during
	// shutdown is still acknowledged.
	settleTimeout = 5 * time.Second

	// receiveRetryDelay spaces out Receive retries while the client recovers
	// a lost connection.
	receiveRetryDelay = time.Second
)

// SubscriberWithMaxAttempts dead-letters or discards a message after n failed
// handler attempts; n < 1 keeps the default of retrying forever.
func SubscriberWithMaxAttempts(n int) SubscriberOpt {
	return func(cfg *subscriberCfg) {
		if n >= 1 {
			cfg.maxAttempts = n
		}
	}
}

// SubscriberWithBackoff sets the delay between handler retries: initial
// before the first retry, doubling up to the cap. Non-positive values keep
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

// SubscriberWithDeadLetterTopic moves exhausted and Permanent messages to
// topic's exchange instead of discarding them, and binds a catch-all queue
// named after it so they survive without a dead-letter consumer.
func SubscriberWithDeadLetterTopic(topic string) SubscriberOpt {
	return func(cfg *subscriberCfg) {
		cfg.deadLetterTopic = topic
	}
}

// NewSubscriber builds a Subscriber delivering topic's messages to h on
// client's connection, owning the queue "<topic>.<name>". Most applications
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

	cfg := subscriberCfg{initialBackoff: defaultInitialBackoff, maxBackoff: defaultMaxBackoff}
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

// Queue returns the name of the queue the subscription owns.
func (s *Subscriber) Queue() string {
	return s.topic + "." + s.name
}

// Subscribe declares the exchange, queue and binding (plus the dead-letter
// topology when configured), attaches a consumer to the client's connection,
// dialing it if needed, and starts the worker.
func (s *Subscriber) Subscribe(ctx context.Context, onError messaging.OnErrorFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.w != nil {
		return messaging.ErrAlreadySubscribed
	}

	conn, err := s.client.connection(ctx)
	if err != nil {
		return err
	}

	w := &worker{done: make(chan struct{}), onError: onError}
	if err := s.attach(ctx, conn, w); err != nil {
		return errors.Join(err, w.detach(ctx))
	}

	runCtx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	s.w = w
	go s.run(runCtx, w)

	return nil
}

// attach declares the topology and opens the worker's links on conn.
func (s *Subscriber) attach(ctx context.Context, conn *rabbitmqamqp.AmqpConnection, w *worker) error {
	if err := declareTopic(ctx, conn, s.topic, s.Queue()); err != nil {
		return err
	}

	if s.cfg.deadLetterTopic != "" {
		// The catch-all queue is named after the dead-letter topic itself,
		// so dead letters are retained even without a dead-letter consumer.
		if err := declareTopic(ctx, conn, s.cfg.deadLetterTopic, s.cfg.deadLetterTopic); err != nil {
			return err
		}
		dlt, err := conn.NewPublisher(ctx, nil, nil)
		if err != nil {
			return err
		}
		w.dlt = dlt
	}

	consumer, err := conn.NewConsumer(ctx, s.Queue(), nil)
	if err != nil {
		return err
	}
	w.consumer = consumer

	return nil
}

// detach closes the worker's links, leaving the shared connection open.
func (w *worker) detach(ctx context.Context) error {
	var errs []error
	if w.consumer != nil {
		if err := w.consumer.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closing the consumer: %w", err))
		}
	}
	if w.dlt != nil {
		if err := w.dlt.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closing the dead-letter publisher: %w", err))
		}
	}

	return errors.Join(errs...)
}

// declareTopic declares topic's fanout exchange, the quorum queue and the
// binding between them.
func declareTopic(ctx context.Context, conn *rabbitmqamqp.AmqpConnection, topic, queue string) error {
	management := conn.Management()

	if _, err := management.DeclareExchange(ctx,
		&rabbitmqamqp.FanOutExchangeSpecification{Name: topic}); err != nil {
		return err
	}
	if _, err := management.DeclareQueue(ctx,
		&rabbitmqamqp.QuorumQueueSpecification{Name: queue}); err != nil {
		return err
	}
	if _, err := management.Bind(ctx, &rabbitmqamqp.ExchangeToQueueBindingSpecification{
		SourceExchange:   topic,
		DestinationQueue: queue,
	}); err != nil {
		return err
	}

	return nil
}

// Unsubscribe stops the worker and detaches its links, waiting until the
// in-flight handler finished or ctx expires. An unsettled message is
// redelivered to the next subscription.
func (s *Subscriber) Unsubscribe(ctx context.Context) error {
	s.mu.Lock()
	w := s.w
	s.w = nil
	s.mu.Unlock()

	if w == nil {
		return messaging.ErrNotSubscribed
	}

	w.cancel()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Subscriber) run(ctx context.Context, w *worker) {
	defer close(w.done)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), settleTimeout)
		defer cancel()

		if err := w.detach(closeCtx); err != nil {
			s.log().Warn(closeCtx, "detaching the subscription: %v", err)
		}
	}()

	for {
		delivery, err := w.consumer.Receive(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.fail(ctx, w, fmt.Errorf("receiving from queue %s: %w", s.Queue(), err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(receiveRetryDelay):
			}
			continue
		}

		s.consume(ctx, w, delivery)
	}
}

// consume processes one delivery, retrying transient handler failures with
// backoff, and settles it once it is handled, dead-lettered or discarded. It
// returns without settling when the subscription is stopping, so the message
// is requeued and redelivered.
func (s *Subscriber) consume(ctx context.Context, w *worker, delivery rabbitmqamqp.IDeliveryContext) {
	msg := toMessage(s.topic, delivery.Message())
	attrs := []attribute.KeyValue{semconv.MessagingDestinationName(s.topic)}
	if msg.Key != "" {
		attrs = append(attrs, semconv.MessagingRabbitMQDestinationRoutingKey(msg.Key))
	}

	for attempts := 1; ; attempts++ {
		err := messaging.Consume(ctx, SystemName, msg, s.handler, attrs...)
		if err == nil {
			s.settle(ctx, w, msg, delivery.Accept)
			return
		}
		if ctx.Err() != nil {
			return
		}
		if messaging.IsPermanent(err) || s.exhausted(attempts) {
			s.park(ctx, w, delivery, msg, err)
			return
		}

		s.log().
			With("msgID", msg.ID).
			With("attempts", attempts).
			Warn(ctx, "message handler failed, retrying: %v", err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(s.backoff(attempts)):
		}
	}
}

// park moves a message the handler can never process to the dead-letter
// exchange, or reports and discards it when none is configured. The delivery
// is only settled once the message is parked, so a failed dead-letter publish
// leads to redelivery, not loss.
func (s *Subscriber) park(ctx context.Context, w *worker, delivery rabbitmqamqp.IDeliveryContext, msg messaging.Message, handlerErr error) {
	if s.cfg.deadLetterTopic == "" {
		s.fail(ctx, w, fmt.Errorf("message %s on topic %s discarded after a non-retryable failure: %w", msg.ID, msg.Topic, handlerErr))
		s.settle(ctx, w, msg, func(settleCtx context.Context) error {
			return delivery.Discard(settleCtx, &amqp.Error{
				Condition:   "messaging:handler-failure",
				Description: handlerErr.Error(),
			})
		})
		return
	}

	dead := msg
	dead.Topic = s.cfg.deadLetterTopic
	dead.Headers = make(map[string]string, len(msg.Headers)+2)
	maps.Copy(dead.Headers, msg.Headers)
	dead.Headers[messaging.HeaderDeadLetterOrigin] = msg.Topic
	dead.Headers[messaging.HeaderDeadLetterReason] = handlerErr.Error()

	if err := s.publishDead(ctx, w, dead); err != nil {
		// Not settled: the message will be redelivered and dead-lettered
		// again after the requeue.
		s.fail(ctx, w, fmt.Errorf("dead-lettering message %s from topic %s: %w", msg.ID, msg.Topic, err))
		s.settle(ctx, w, msg, delivery.Requeue)
		return
	}

	s.log().With("msgID", msg.ID).Warn(ctx, "message dead-lettered to %s: %v", s.cfg.deadLetterTopic, handlerErr)
	s.settle(ctx, w, msg, delivery.Accept)
}

func (s *Subscriber) publishDead(ctx context.Context, w *worker, dead messaging.Message) error {
	out, err := toAmqpMessage(dead)
	if err != nil {
		return err
	}

	res, err := w.dlt.Publish(ctx, out)
	if err != nil {
		return err
	}
	if _, accepted := res.Outcome.(*rabbitmqamqp.StateAccepted); !accepted {
		return fmt.Errorf("dead-letter outcome %T", res.Outcome)
	}

	return nil
}

// settle runs one settlement call on its own timeout, so shutdown does not
// lose an already-handled message, and reports failures: an unsettled
// message is requeued and redelivered.
func (s *Subscriber) settle(ctx context.Context, w *worker, msg messaging.Message, op func(context.Context) error) {
	settleCtx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()

	if err := op(settleCtx); err != nil {
		s.fail(ctx, w, fmt.Errorf("settling message %s on topic %s: %w", msg.ID, msg.Topic, err))
	}
}

func (s *Subscriber) exhausted(attempts int) bool {
	return s.cfg.maxAttempts > 0 && attempts >= s.cfg.maxAttempts
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
