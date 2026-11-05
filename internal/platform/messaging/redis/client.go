package redis

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/logging"
)

type (
	// Config connects the package to a Redis server.
	Config struct {
		// URL is the server endpoint, e.g. redis://user:pass@host:6379/0.
		URL string `conf:"default:redis://localhost:6379/0"`
		// StreamMaxLen approximately caps the entries retained per topic
		// stream, trimming the oldest on publish; 0 retains everything.
		StreamMaxLen int64 `conf:"default:0"`
	}

	// Client owns the one go-redis client (a connection pool) a service
	// shares. Open verifies the server is reachable and Close releases the
	// pool. Most applications wire it with Module.
	Client struct {
		cfg  Config
		opts *redis.Options
		log  logging.Logger

		mu  sync.Mutex
		rdb *redis.Client
	}
)

// NewClient builds a Client on cfg, reporting server problems to log.
func NewClient(cfg Config, log logging.Logger) (*Client, error) {
	if cfg.URL == "" {
		return nil, fields.NewErrInvalidEmptyString(fields.NameURL)
	}

	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fields.NewErrWithFieldName(fields.NameURL, err)
	}
	// Honour the deadlines of the contexts handed to blocking reads.
	opts.ContextTimeoutEnabled = true

	return &Client{cfg: cfg, opts: opts, log: log}, nil
}

// Open creates the pool if needed and pings the server.
func (c *Client) Open(ctx context.Context) error {
	return c.connection().Ping(ctx).Err()
}

// Close releases the pool, failing the reads of every active subscription.
// It is a no-op on a closed client, which can be opened again.
func (c *Client) Close(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.rdb == nil {
		return nil
	}

	err := c.rdb.Close()
	c.rdb = nil

	return err
}

// connection returns the shared go-redis client, creating it on first use;
// connections are dialed lazily by the pool.
func (c *Client) connection() *redis.Client {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.rdb == nil {
		c.rdb = redis.NewClient(c.opts)
	}

	return c.rdb
}

// xaddArgs builds the XADD arguments storing values on stream with the
// configured retention.
func (c *Client) xaddArgs(stream string, values map[string]any) *redis.XAddArgs {
	args := &redis.XAddArgs{Stream: stream, Values: values}
	if c.cfg.StreamMaxLen > 0 {
		args.MaxLen = c.cfg.StreamMaxLen
		args.Approx = true
	}

	return args
}
