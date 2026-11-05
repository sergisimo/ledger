package messagingtest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/sergisimo/ledger/internal/platform/logging"
	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// ModuleTimeout bounds the delivery wait of AssertModuleRoundTrip; networked
// brokers need a few coordination rounds before a subscription receives.
const ModuleTimeout = 10 * time.Second

type (
	// ModuleCase describes the broker wiring AssertModuleRoundTrip exercises.
	ModuleCase struct {
		// Topic and ConsumerName identify the subscription under test; they
		// must match the ones given to the broker's SubscriberFx in Options.
		Topic        string
		ConsumerName string
		// PublisherName and System are handed to messaging.PublisherFx: the
		// broker's PublisherName (or outbox.PublisherName) and SystemName.
		PublisherName string
		System        string
		// Options wire the broker: its Module, its SubscriberFx for
		// ConsumerName and Topic, and any dependency they need.
		Options []fx.Option
	}

	moduleEvent struct {
		Name string `json:"name"`
	}
)

// AssertModule boots an fx application from logging, tc.Options, a
// handler provided under tc.ConsumerName and a typed publisher for tc.Topic,
// publishes one event and asserts the handler received it. The application
// stops at cleanup, so callers may assert on broker state after it returns.
func AssertModule(t *testing.T, tc ModuleCase) {
	t.Helper()
	t.Setenv("SERVICE_NAME", "messagingtest")

	received := make(chan moduleEvent, 1)
	newHandler := func() messaging.HandlerFunc {
		return messaging.Handle(func(_ context.Context, payload moduleEvent, _ messaging.Message) error {
			received <- payload
			return nil
		})
	}

	var pub messaging.Publisher[moduleEvent]
	opts := []fx.Option{logging.Module()}
	opts = append(opts, tc.Options...)
	opts = append(opts,
		messaging.HandlerFx(newHandler, tc.ConsumerName),
		messaging.PublisherFx[moduleEvent](tc.Topic, tc.PublisherName, tc.System),
		fx.Populate(&pub),
		fx.NopLogger,
	)

	app := fxtest.New(t, opts...)
	app.RequireStart()
	t.Cleanup(app.RequireStop)

	require.NoError(t, pub.Publish(t.Context(), moduleEvent{Name: "checking"}))

	select {
	case got := <-received:
		assert.Equal(t, moduleEvent{Name: "checking"}, got)
	case <-time.After(ModuleTimeout):
		t.Fatal("the subscriber never received the published event")
	}
}
