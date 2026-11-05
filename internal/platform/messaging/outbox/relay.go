package outbox

import (
	"context"
	"errors"
	"expvar"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/messaging"
	"github.com/sergisimo/ledger/internal/platform/persistence"
)

type (
	// RelayConfig tunes the Relay. The zero value gets sensible defaults,
	// except Retention: zero retention deletes rows as soon as they are
	// dispatched instead of keeping them for later inspection.
	RelayConfig struct {
		// PollInterval is the idle cadence; a full batch triggers an immediate
		// next fetch regardless.
		PollInterval time.Duration `conf:"default:1s"`
		// BatchSize is the maximum number of messages fetched per tick.
		BatchSize int `conf:"default:100"`
		// InitialBackoff delays the first retry; later retries double it.
		InitialBackoff time.Duration `conf:"default:1s"`
		// MaxBackoff caps the retry delay.
		MaxBackoff time.Duration `conf:"default:1m"`
		// MaxAttempts parks a message after that many failed deliveries;
		// 0 retries forever.
		MaxAttempts int `conf:"default:0"`
		// Retention is how long dispatched rows are kept; 0 deletes them on
		// dispatch.
		Retention time.Duration `conf:"default:24h"`
		// CleanupInterval is the cadence of the retention janitor; 0 disables
		// it.
		CleanupInterval time.Duration `conf:"default:1h"`
	}

	// Relay moves pending messages from the Store to the broker. Each tick
	// runs in one transaction: fetch with row locks, publish the batch in a
	// single call, mark dispatched; failed messages (every message on a plain
	// publish error, only the reported ones on a messaging.BatchError) are
	// rescheduled with exponential backoff. Running multiple relays (or
	// service replicas) is safe. Most applications wire it with Module.
	Relay struct {
		store    Store
		delegate messaging.MessagePublisher
		txer     persistence.Transactioner
		log      logging.Logger
		cfg      RelayConfig
		clock    func() time.Time
		stopCh   chan struct{}

		mu    sync.Mutex
		state state
		wg    sync.WaitGroup
	}

	// RelayOpt configures a Relay. See the RelayWith functions.
	RelayOpt func(*Relay)

	state int
)

const (
	stateIdle state = iota
	stateRunning
	stateStopped
)

const (
	defaultPollInterval   = time.Second
	defaultBatchSize      = 100
	defaultInitialBackoff = time.Second
	defaultMaxBackoff     = time.Minute
)

// ErrRelayClosed is returned by Start after Stop; a relay cannot be restarted.
var ErrRelayClosed = errors.New("outbox relay is closed")

// Metrics exposed under /debug/vars via the metrics module.
var (
	dispatchedMessages = expvar.NewInt("messaging.outbox.dispatched")
	retriedMessages    = expvar.NewInt("messaging.outbox.retried")
	exhaustedMessages  = expvar.NewInt("messaging.outbox.exhausted")
	prunedMessages     = expvar.NewInt("messaging.outbox.pruned")
)

var tracer = otel.Tracer("messaging/outbox", trace.WithSchemaURL(semconv.SchemaURL))

// RelayWithClock overrides the relay's clock, e.g. with outboxtest.Clock for
// deterministic backoff tests.
func RelayWithClock(clock func() time.Time) RelayOpt {
	return func(r *Relay) {
		if clock != nil {
			r.clock = clock
		}
	}
}

// NewRelay builds a Relay that fetches from store inside txer transactions and
// publishes to delegate, the broker's MessagePublisher.
func NewRelay(
	store Store,
	delegate messaging.MessagePublisher,
	txer persistence.Transactioner,
	log logging.Logger,
	cfg RelayConfig,
	opts ...RelayOpt,
) *Relay {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = defaultInitialBackoff
	}
	if cfg.MaxBackoff < cfg.InitialBackoff {
		cfg.MaxBackoff = max(defaultMaxBackoff, cfg.InitialBackoff)
	}
	if cfg.MaxAttempts < 0 {
		cfg.MaxAttempts = 0
	}
	if cfg.Retention < 0 {
		cfg.Retention = 0
	}
	if cfg.CleanupInterval < 0 {
		cfg.CleanupInterval = 0
	}

	r := &Relay{
		store:    store,
		delegate: delegate,
		txer:     txer,
		log:      log,
		cfg:      cfg,
		clock:    time.Now,
		stopCh:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Start launches the relay and janitor loops. It is a no-op on a running relay
// and returns ErrRelayClosed on a stopped one.
func (r *Relay) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.state {
	case stateRunning:
		return nil
	case stateStopped:
		return ErrRelayClosed
	case stateIdle:
	}
	r.state = stateRunning

	r.wg.Add(1)
	go r.loop()

	if r.cfg.CleanupInterval > 0 && r.cfg.Retention > 0 {
		r.wg.Add(1)
		go r.janitor()
	}

	return nil
}

// Stop halts the loops, waiting for an in-flight tick until ctx expires. It is
// idempotent.
func (r *Relay) Stop(ctx context.Context) error {
	r.mu.Lock()
	if r.state == stateStopped {
		r.mu.Unlock()
		return nil
	}
	r.state = stateStopped
	r.mu.Unlock()

	close(r.stopCh)

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.wg.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Relay) loop() {
	defer r.wg.Done()

	ctx := context.Background()
	for {
		r.drain(ctx)

		select {
		case <-r.stopCh:
			return
		case <-time.After(r.cfg.PollInterval):
		}
	}
}

func (r *Relay) drain(ctx context.Context) {
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		full, err := r.tick(ctx)
		if err != nil {
			r.log.Error(ctx, "outbox relay tick failed: %v", err)
			return
		}
		if !full {
			return
		}
	}
}

func (r *Relay) tick(ctx context.Context) (bool, error) {
	var (
		full                           bool
		dispatched, retried, exhausted int64
	)

	err := r.txer.Exec(ctx, func(txCtx context.Context) error {
		pending, err := r.store.FetchPending(txCtx, r.clock(), r.cfg.BatchSize, r.cfg.MaxAttempts)
		if err != nil {
			return err
		}
		full = len(pending) == r.cfg.BatchSize
		if len(pending) == 0 {
			return nil
		}

		failures := r.dispatch(txCtx, pending)

		dispatchedIDs := make([]string, 0, len(pending))
		for i, msg := range pending {
			if pubErr, failed := failures[i]; failed {
				retried++
				if r.exhausted(msg.Attempts + 1) {
					exhausted++
				}
				if err := r.reschedule(txCtx, msg, pubErr); err != nil {
					return err
				}
				continue
			}
			dispatchedIDs = append(dispatchedIDs, msg.ID)
		}
		dispatched = int64(len(dispatchedIDs))

		if len(dispatchedIDs) == 0 {
			return nil
		}
		if r.cfg.Retention == 0 {
			return r.store.Delete(txCtx, dispatchedIDs...)
		}

		return r.store.MarkDispatched(txCtx, r.clock(), dispatchedIDs...)
	})
	if err != nil {
		return full, err
	}

	dispatchedMessages.Add(dispatched)
	retriedMessages.Add(retried)
	exhaustedMessages.Add(exhausted)

	return full, nil
}

// dispatch publishes the whole batch in one call and returns the failures by
// batch index: nil on success, only the failed indexes when the delegate
// reports a messaging.BatchError, and every index on any other error.
func (r *Relay) dispatch(ctx context.Context, pending []Pending) map[int]error {
	msgs := make([]messaging.Message, len(pending))
	links := make([]trace.Link, 0, len(pending))
	for i, msg := range pending {
		msgs[i] = msg.Message
		// A batch mixes messages from unrelated transactions, so the span
		// links each publishing trace instead of joining one of them.
		if sc := trace.SpanContextFromContext(messaging.ExtractTrace(context.Background(), msg.Message)); sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}

	ctx, span := tracer.Start(
		ctx,
		"outbox.dispatch",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithLinks(links...),
		trace.WithAttributes(
			semconv.MessagingOperationName("dispatch"),
			semconv.MessagingOperationTypeSend,
			semconv.MessagingBatchMessageCount(len(msgs)),
		),
	)
	defer span.End()

	err := r.delegate.Publish(ctx, msgs...)
	if err == nil {
		return nil
	}
	span.RecordError(err)

	failures := make(map[int]error, len(pending))
	if batchErr, ok := errors.AsType[*messaging.BatchError](err); ok {
		for i, pubErr := range batchErr.Failed {
			if i >= 0 && i < len(pending) {
				failures[i] = pubErr
			}
		}
	}
	if len(failures) == 0 {
		for i := range pending {
			failures[i] = err
		}
	}

	return failures
}

func (r *Relay) reschedule(ctx context.Context, msg Pending, pubErr error) error {
	attempts := msg.Attempts + 1

	log := r.log.With("topic", msg.Topic).With("msgID", msg.ID).With("attempts", attempts)
	if r.exhausted(attempts) {
		log.Error(ctx, "outbox message exhausted its attempts: %v", pubErr)
	} else {
		log.Warn(ctx, "outbox message failed to publish: %v", pubErr)
	}

	return r.store.Reschedule(ctx, msg.ID, attempts, r.clock().Add(r.backoff(attempts)), pubErr.Error())
}

func (r *Relay) exhausted(attempts int) bool {
	return r.cfg.MaxAttempts > 0 && attempts >= r.cfg.MaxAttempts
}

func (r *Relay) backoff(attempts int) time.Duration {
	d := r.cfg.InitialBackoff
	for i := 1; i < attempts; i++ {
		d *= 2
		if d <= 0 || d >= r.cfg.MaxBackoff {
			return r.cfg.MaxBackoff
		}
	}

	return min(d, r.cfg.MaxBackoff)
}

func (r *Relay) janitor() {
	defer r.wg.Done()

	ctx := context.Background()
	ticker := time.NewTicker(r.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			deleted, err := r.store.DeleteDispatchedBefore(ctx, r.clock().Add(-r.cfg.Retention))
			if err != nil {
				r.log.Error(ctx, "outbox janitor failed: %v", err)
				continue
			}
			prunedMessages.Add(deleted)
		}
	}
}
