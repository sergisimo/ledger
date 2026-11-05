package kafka

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/messaging"
)

type (
	// Subscriber is a messaging.Subscriber bound to one topic, consumer group
	// and handler. Each Subscribe opens a dedicated consumer-group client and
	// processes the fetched records in order on one worker, acknowledging by
	// marking offsets; see the package documentation for the retry and
	// dead-letter semantics. Handlers run on a context carrying the
	// publishing trace.
	Subscriber struct {
		client  *Client
		topic   string
		group   string
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
		cl      *kgo.Client
		cancel  context.CancelFunc
		done    chan struct{}
		onError messaging.OnErrorFunc
	}
)

const (
	defaultInitialBackoff = time.Second
	defaultMaxBackoff     = time.Minute

	// shutdownCommitTimeout bounds the final marked-offset commit when a
	// subscription stops.
	shutdownCommitTimeout = 5 * time.Second
)

// SubscriberWithMaxAttempts dead-letters or drops a message after n failed
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
// topic instead of dropping them.
func SubscriberWithDeadLetterTopic(topic string) SubscriberOpt {
	return func(cfg *subscriberCfg) {
		cfg.deadLetterTopic = topic
	}
}

// NewSubscriber builds a Subscriber delivering topic's messages to h on
// client's cluster, as the consumer group named group. Most applications wire
// it with messaging.SubscriberFx, passing the consumer name as the group.
func NewSubscriber(client *Client, topic, group string, h messaging.HandlerFunc, opts ...SubscriberOpt) (*Subscriber, error) {
	if client == nil {
		return nil, fields.NewErrNil(fields.NameClient)
	}
	if topic == "" {
		return nil, fields.NewErrInvalidEmptyString(messaging.FieldNameTopic)
	}
	if group == "" {
		return nil, fields.NewErrInvalidEmptyString(messaging.FieldNameConsumer)
	}
	if h == nil {
		return nil, fields.NewErrNil(messaging.FieldNameHandler)
	}

	cfg := subscriberCfg{initialBackoff: defaultInitialBackoff, maxBackoff: defaultMaxBackoff}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.maxBackoff < cfg.initialBackoff {
		cfg.maxBackoff = cfg.initialBackoff
	}

	return &Subscriber{client: client, topic: topic, group: group, handler: h, cfg: cfg}, nil
}

// Subscribe joins the consumer group and starts the worker, failing if the
// cluster is unreachable.
func (s *Subscriber) Subscribe(ctx context.Context, onError messaging.OnErrorFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.w != nil {
		return messaging.ErrAlreadySubscribed
	}

	cl, err := kgo.NewClient(
		append(
			s.client.opts(),
			kgo.ConsumerGroup(s.group),
			kgo.ConsumeTopics(s.topic),
			kgo.AutoCommitMarks(),
		)...)
	if err != nil {
		return err
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	w := &worker{cl: cl, cancel: cancel, done: make(chan struct{}), onError: onError}
	s.w = w
	go s.run(runCtx, w)

	return nil
}

// Unsubscribe stops the worker and leaves the group, waiting until the
// in-flight handler finished and the marked offsets are committed, or ctx
// expires. The worker keeps its own shutdown deadline, so an expired ctx only
// abandons the wait.
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
		commitCtx, cancel := context.WithTimeout(context.Background(), shutdownCommitTimeout)
		defer cancel()
		if err := w.cl.CommitMarkedOffsets(commitCtx); err != nil {
			s.log().Warn(commitCtx, "committing marked offsets on shutdown: %v", err)
		}
		w.cl.Close()
	}()

	for {
		fetches := w.cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			s.fail(ctx, w, fmt.Errorf("fetching topic %s partition %d: %w", topic, partition, err))
		})
		fetches.EachRecord(func(rec *kgo.Record) {
			if ctx.Err() != nil {
				return
			}
			s.consume(ctx, w, rec)
		})
	}
}

// consume processes one record, retrying transient handler failures with
// backoff, and marks it once it is handled, dead-lettered or dropped. It
// returns without marking when the subscription is stopping, so the record is
// redelivered later.
func (s *Subscriber) consume(ctx context.Context, w *worker, rec *kgo.Record) {
	msg := toMessage(rec)
	attrs := []attribute.KeyValue{
		semconv.MessagingConsumerGroupName(s.group),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(int(rec.Partition))),
		semconv.MessagingKafkaOffset(int(rec.Offset)),
	}
	if msg.Key != "" {
		attrs = append(attrs, semconv.MessagingKafkaMessageKey(msg.Key))
	}

	for attempts := 1; ; attempts++ {
		err := messaging.Consume(ctx, SystemName, msg, s.handler, attrs...)
		if err == nil {
			w.cl.MarkCommitRecords(rec)
			return
		}
		if ctx.Err() != nil {
			return
		}
		if messaging.IsPermanent(err) || s.exhausted(attempts) {
			s.park(ctx, w, rec, msg, err)
			return
		}

		s.log().With("msgID", msg.ID).With("attempts", attempts).Warn(ctx, "message handler failed, retrying: %v", err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(s.backoff(attempts)):
		}
	}
}

// park moves a message the handler can never process to the dead-letter
// topic, or reports and drops it when none is configured. The record is only
// marked once the message is parked, so a failed dead-letter produce leads to
// redelivery, not loss.
func (s *Subscriber) park(ctx context.Context, w *worker, rec *kgo.Record, msg messaging.Message, handlerErr error) {
	if s.cfg.deadLetterTopic == "" {
		s.fail(ctx, w, fmt.Errorf("message %s on topic %s dropped after a non-retryable failure: %w", msg.ID, msg.Topic, handlerErr))
		w.cl.MarkCommitRecords(rec)
		return
	}

	dead := msg
	dead.Topic = s.cfg.deadLetterTopic
	dead.Headers = make(map[string]string, len(msg.Headers)+2)
	maps.Copy(dead.Headers, msg.Headers)
	dead.Headers[messaging.HeaderDeadLetterOrigin] = msg.Topic
	dead.Headers[messaging.HeaderDeadLetterReason] = handlerErr.Error()

	if err := w.cl.ProduceSync(ctx, toRecord(dead)).FirstErr(); err != nil {
		s.fail(ctx, w, fmt.Errorf("dead-lettering message %s from topic %s: %w", msg.ID, msg.Topic, err))
		return
	}

	s.log().With("msgID", msg.ID).Warn(ctx, "message dead-lettered to %s: %v", s.cfg.deadLetterTopic, handlerErr)
	w.cl.MarkCommitRecords(rec)
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
	return s.client.log.With("topic", s.topic).With("group", s.group)
}
