package rabbitmq

import (
	"context"
	"sync"

	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/logging"
)

type (
	// Config connects the package to a RabbitMQ 4.x broker.
	Config struct {
		// URL is the AMQP endpoint, including credentials and virtual host.
		URL string `conf:"default:amqp://guest:guest@localhost:5672/"`
		// ContainerID identifies this service's connection to the broker,
		// e.g. the service name; empty lets the client generate one.
		ContainerID string `conf:"default:"`
	}

	// Client owns the one AMQP connection a service shares: AMQP 1.0
	// multiplexes every publisher and consumer over it on their own links.
	// Open dials it (or the first publisher or subscriber does lazily) and
	// Close releases it. Most applications wire it with Module.
	Client struct {
		cfg Config
		log logging.Logger

		mu   sync.Mutex
		conn *rabbitmqamqp.AmqpConnection
	}
)

// NewClient builds a Client on cfg, reporting broker problems to log.
func NewClient(cfg Config, log logging.Logger) (*Client, error) {
	if cfg.URL == "" {
		return nil, fields.NewErrInvalidEmptyString(fields.NameURL)
	}

	return &Client{cfg: cfg, log: log}, nil
}

// Open dials the broker with automatic recovery enabled. It is a no-op on an
// open client.
func (c *Client) Open(ctx context.Context) error {
	_, err := c.connection(ctx)
	return err
}

// Close closes the shared connection, and with it every publisher and
// consumer attached to it. It is a no-op on a closed client, which can be
// opened again.
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil
	}

	err := c.conn.Close(ctx)
	c.conn = nil

	return err
}

// connection returns the shared connection, dialing it on first use.
func (c *Client) connection(ctx context.Context) (*rabbitmqamqp.AmqpConnection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn, nil
	}

	conn, err := rabbitmqamqp.Dial(ctx, c.cfg.URL, &rabbitmqamqp.AmqpConnOptions{
		ContainerID: c.cfg.ContainerID,
	})
	if err != nil {
		return nil, err
	}
	c.conn = conn

	return conn, nil
}
