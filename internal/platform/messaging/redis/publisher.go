package redis

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// Publisher is a messaging.MessagePublisher appending to topic streams in
// one pipeline per batch on the client's shared pool, so a batch may span
// topics. Most applications wire it with Module.
type Publisher struct {
	client *Client
}

// NewPublisher builds a Publisher on client's pool.
func NewPublisher(client *Client) *Publisher {
	return &Publisher{client: client}
}

// Open verifies the server is reachable.
func (p *Publisher) Open(ctx context.Context) error {
	return p.client.Open(ctx)
}

// Publish appends msgs to their streams and waits for the server's reply to
// every one of them. When only part of the batch is appended the rest is
// reported with a *messaging.BatchError.
func (p *Publisher) Publish(ctx context.Context, msgs ...messaging.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	for _, msg := range msgs {
		if err := msg.Validate(); err != nil {
			return err
		}
	}

	cmds := make([]*redis.StringCmd, len(msgs))
	_, pipeErr := p.client.connection().Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, msg := range msgs {
			cmds[i] = pipe.XAdd(ctx, p.client.xaddArgs(msg.Topic, toValues(msg)))
		}
		return nil
	})

	failed := map[int]error{}
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			failed[i] = err
		}
	}
	if len(failed) == 0 {
		return pipeErr
	}

	return &messaging.BatchError{Failed: failed}
}
