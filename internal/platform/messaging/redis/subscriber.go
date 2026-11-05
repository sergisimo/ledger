package redis

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/messaging"
)

type (
	// Subscriber is a messaging.Subscriber bound to one topic, consumer group
	// and handler. Each Subscribe joins the group as a uniquely named
	// consumer and processes the entries it reads, and the stale ones it
	// reclaims, in order on one worker; see the package documentation for
	// the retry and dead-letter semantics. Handlers run on a context
	// carrying the publishing trace.
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
		claimIdle       time.Duration
		deadLetterTopic string
	}

	worker struct {
		rdb      *redis.Client
		consumer string
		cancel   context.CancelFunc
		done     chan struct{}
		onError  messaging.OnErrorFunc
	}
)

const (
	defaultInitialBackoff = time.Second
	defaultMaxBackoff     = time.Minute
	defaultClaimIdle      = time.Minute

	// prefetch is how many entries one read or claim fetches.
	prefetch = 16

	// blockTimeout bounds each blocking read, so the worker regularly checks
	// for cancellation and for entries to reclaim.
	blockTimeout = time.Second

	// settleTimeout bounds every acknowledgement and dead-letter append, so
	// a message handled during shutdown is still settled.
	settleTimeout = 5 * time.Second

	// readRetryDelay spaces out reads failing while the server is unreachable.
	readRetryDelay = time.Second
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

// SubscriberWithClaimIdle sets how long an entry may stay pending on one
// consumer before the group's other instances reclaim it; keep it above the
// slowest handler including its retries. Non-positive values keep the
// default of 1m.
func SubscriberWithClaimIdle(d time.Duration) SubscriberOpt {
	return func(cfg *subscriberCfg) {
		if d > 0 {
			cfg.claimIdle = d
		}
	}
}

// SubscriberWithDeadLetterTopic moves exhausted and Permanent messages to
// topic's stream instead of dropping them.
func SubscriberWithDeadLetterTopic(topic string) SubscriberOpt {
	return func(cfg *subscriberCfg) {
		cfg.deadLetterTopic = topic
	}
}

// NewSubscriber builds a Subscriber delivering topic's messages to h on
// client's pool, as the consumer group named name. Most applications wire it
// with messaging.SubscriberFx, passing the consumer name as name.
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
		claimIdle:      defaultClaimIdle,
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

// Subscribe creates the consumer group (and the stream when missing) and
// starts the worker, failing if the server is unreachable.
func (s *Subscriber) Subscribe(ctx context.Context, onError messaging.OnErrorFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.w != nil {
		return messaging.ErrAlreadySubscribed
	}

	rdb := s.client.connection()
	if err := createGroup(ctx, rdb, s.topic, s.name); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	w := &worker{rdb: rdb, consumer: consumerName(), cancel: cancel, done: make(chan struct{}), onError: onError}
	s.w = w
	go s.run(runCtx, w)

	return nil
}

// createGroup declares group on stream from its first entry, creating the
// stream when missing; an existing group is left as is.
func createGroup(ctx context.Context, rdb *redis.Client, stream, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}

	return nil
}

// consumerName identifies this instance inside the group.
func consumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "consumer"
	}

	return host + "-" + uuid.NewString()
}

// Unsubscribe stops the worker, waiting until the in-flight handler finished
// or ctx expires. An unacknowledged entry stays pending and is reclaimed by
// the group's other instances once idle for the claim time.
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

	var lastClaim time.Time
	for {
		if ctx.Err() != nil {
			return
		}

		if time.Since(lastClaim) >= s.claimInterval() {
			s.claim(ctx, w)
			lastClaim = time.Now()
		}

		streams, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.name,
			Consumer: w.consumer,
			Streams:  []string{s.topic, ">"},
			Count:    prefetch,
			Block:    blockTimeout,
		}).Result()
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			s.fail(ctx, w, fmt.Errorf("reading stream %s as group %s: %w", s.topic, s.name, err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(readRetryDelay):
			}
			continue
		}

		for _, stream := range streams {
			for _, entry := range stream.Messages {
				if ctx.Err() != nil {
					return
				}
				s.consume(ctx, w, entry)
			}
		}
	}
}

// claimInterval is how often the worker looks for entries to reclaim.
func (s *Subscriber) claimInterval() time.Duration {
	return max(s.cfg.claimIdle/2, 100*time.Millisecond)
}

// claim takes over the group's entries left pending for longer than the
// claim time by other instances and processes them.
func (s *Subscriber) claim(ctx context.Context, w *worker) {
	entries, _, err := w.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   s.topic,
		Group:    s.name,
		Consumer: w.consumer,
		MinIdle:  s.cfg.claimIdle,
		Start:    "0-0",
		Count:    prefetch,
	}).Result()
	if err != nil {
		if ctx.Err() == nil {
			s.fail(ctx, w, fmt.Errorf("reclaiming pending entries of stream %s as group %s: %w", s.topic, s.name, err))
		}
		return
	}

	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if len(entry.Values) == 0 {
			// The entry was trimmed from the stream; nothing left to process.
			s.settle(ctx, w, entry.ID, s.ack(w, entry.ID))
			continue
		}
		s.consume(ctx, w, entry)
	}
}

// consume processes one entry, retrying transient handler failures with
// backoff, and acknowledges it once it is handled, dead-lettered or dropped.
// It returns without acknowledging when the subscription is stopping, so the
// entry stays pending and is reclaimed later.
func (s *Subscriber) consume(ctx context.Context, w *worker, entry redis.XMessage) {
	msg := toMessage(s.topic, entry)
	attrs := []attribute.KeyValue{semconv.MessagingConsumerGroupName(s.name)}

	for attempts := 1; ; attempts++ {
		err := messaging.Consume(ctx, SystemName, msg, s.handler, attrs...)
		if err == nil {
			s.settle(ctx, w, msg.ID, s.ack(w, entry.ID))
			return
		}
		if ctx.Err() != nil {
			return
		}
		if messaging.IsPermanent(err) || s.exhausted(attempts) {
			s.park(ctx, w, entry, msg, err)
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
// stream, or reports and drops it when none is configured. The entry is only
// acknowledged once the message is parked, so a failed dead-letter append
// leaves it pending to be reclaimed, not lost.
func (s *Subscriber) park(ctx context.Context, w *worker, entry redis.XMessage, msg messaging.Message, handlerErr error) {
	if s.cfg.deadLetterTopic == "" {
		s.fail(ctx, w, fmt.Errorf("message %s on topic %s dropped after a non-retryable failure: %w", msg.ID, msg.Topic, handlerErr))
		s.settle(ctx, w, msg.ID, s.ack(w, entry.ID))
		return
	}

	dead := msg
	dead.Topic = s.cfg.deadLetterTopic
	dead.Headers = make(map[string]string, len(msg.Headers)+2)
	maps.Copy(dead.Headers, msg.Headers)
	dead.Headers[messaging.HeaderDeadLetterOrigin] = msg.Topic
	dead.Headers[messaging.HeaderDeadLetterReason] = handlerErr.Error()

	appendCtx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	if err := w.rdb.XAdd(appendCtx, s.client.xaddArgs(dead.Topic, toValues(dead))).Err(); err != nil {
		s.fail(ctx, w, fmt.Errorf("dead-lettering message %s from topic %s: %w", msg.ID, msg.Topic, err))
		return
	}

	s.log().With("msgID", msg.ID).Warn(ctx, "message dead-lettered to %s: %v", s.cfg.deadLetterTopic, handlerErr)
	s.settle(ctx, w, msg.ID, s.ack(w, entry.ID))
}

// ack returns the acknowledgement of entryID for settle.
func (s *Subscriber) ack(w *worker, entryID string) func(context.Context) error {
	return func(ctx context.Context) error {
		return w.rdb.XAck(ctx, s.topic, s.name, entryID).Err()
	}
}

// settle runs one acknowledgement on its own timeout, so shutdown does not
// lose an already-handled message, and reports failures: an unacknowledged
// entry stays pending and is reclaimed later.
func (s *Subscriber) settle(ctx context.Context, w *worker, msgID string, op func(context.Context) error) {
	settleCtx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()

	if err := op(settleCtx); err != nil {
		s.fail(ctx, w, fmt.Errorf("acknowledging message %s on topic %s: %w", msgID, s.topic, err))
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
