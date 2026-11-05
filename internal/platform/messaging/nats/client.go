package nats

import (
	"context"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/logging"
)

type (
	// Config connects the package to a NATS server with JetStream enabled.
	Config struct {
		// URL is the server endpoint; a comma-separated list is accepted.
		URL string `conf:"default:nats://127.0.0.1:4222"`
		// Name identifies this service's connection to the server, e.g. the
		// service name; empty leaves it unnamed.
		Name string `conf:"default:"`
	}

	// Client owns the one connection a service shares. Open dials it (or the
	// first publisher or subscriber does lazily) and Close releases it. Most
	// applications wire it with Module.
	Client struct {
		cfg Config
		log logging.Logger

		mu sync.Mutex
		js jetstream.JetStream
	}
)

// NewClient builds a Client on cfg, reporting server problems to log.
func NewClient(cfg Config, log logging.Logger) (*Client, error) {
	if cfg.URL == "" {
		return nil, fields.NewErrInvalidEmptyString(fields.NameURL)
	}

	return &Client{cfg: cfg, log: log}, nil
}

// Open connects to the server with unlimited reconnects. It is a no-op on an
// open client.
func (c *Client) Open(ctx context.Context) error {
	_, err := c.jetstream(ctx)
	return err
}

// Close flushes and closes the shared connection, and with it every
// subscription attached to it. It is a no-op on a closed client, which can
// be opened again.
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.js == nil || c.js.Conn() == nil {
		return nil
	}

	err := c.js.Conn().FlushWithContext(ctx)
	c.js.Conn().Close()
	c.js = nil

	return err
}

// jetstream returns the shared JetStream context, connecting on first use.
func (c *Client) jetstream(_ context.Context) (jetstream.JetStream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.js != nil {
		return c.js, nil
	}

	opts := []nats.Option{nats.MaxReconnects(-1)}
	if c.cfg.Name != "" {
		opts = append(opts, nats.Name(c.cfg.Name))
	}

	nc, err := nats.Connect(c.cfg.URL, opts...)
	if err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	c.js = js

	return js, nil
}

// streamName derives the stream holding topic: stream names cannot contain
// dots, spaces or wildcards, so they become underscores. Topics differing
// only by those characters would share a stream; avoid them.
func streamName(topic string) string {
	return strings.NewReplacer(".", "_", " ", "_", "*", "_", ">", "_", "/", "_").Replace(topic)
}

// ensureStream declares the file-backed stream capturing topic, updating an
// existing one to the same configuration.
func ensureStream(ctx context.Context, js jetstream.JetStream, topic string) (jetstream.Stream, error) {
	return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      streamName(topic),
		Subjects:  []string{topic},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
	})
}
