package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sergisimo/ledger/internal/platform/fields"
	"github.com/sergisimo/ledger/internal/platform/logging"
)

const FieldNameBrokers fields.Name = "brokers"

type (
	// Config connects the package to a Kafka cluster.
	Config struct {
		// Brokers is the seed broker list.
		Brokers []string `conf:"default:localhost:9092"`
		// ClientID identifies this service to the cluster, e.g. the service
		// name; empty keeps the client library's default.
		ClientID string `conf:"default:"`
		// AllowAutoTopicCreation lets produces create missing topics when the
		// cluster permits it.
		AllowAutoTopicCreation bool `conf:"default:false"`
	}

	// Client carries the cluster configuration the package's publishers and
	// subscribers build their connections from. Most applications wire it
	// with Module.
	Client struct {
		cfg Config
		log logging.Logger
	}

	// kgoLogger forwards franz-go's internal warnings and errors to the
	// platform logger.
	kgoLogger struct {
		log logging.Logger
	}
)

// NewClient builds a Client on cfg, reporting broker client problems to log.
func NewClient(cfg Config, log logging.Logger) (*Client, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fields.NewErrZeroVal(FieldNameBrokers)
	}

	return &Client{cfg: cfg, log: log}, nil
}

// opts returns the kgo options shared by every connection of this client.
func (c *Client) opts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.cfg.Brokers...),
		kgo.WithLogger(kgoLogger{log: c.log}),
	}
	if c.cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(c.cfg.ClientID))
	}
	if c.cfg.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	return opts
}

func (l kgoLogger) Level() kgo.LogLevel {
	return kgo.LogLevelWarn
}

func (l kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	log := l.log
	for i := 0; i+1 < len(keyvals); i += 2 {
		log = log.With(fmt.Sprint(keyvals[i]), keyvals[i+1])
	}

	ctx := context.Background()
	if level <= kgo.LogLevelError {
		log.Error(ctx, "kafka client: %s", msg)
		return
	}
	log.Warn(ctx, "kafka client: %s", msg)
}
