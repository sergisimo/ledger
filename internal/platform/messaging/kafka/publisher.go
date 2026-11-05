package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// Publisher is a messaging.MessagePublisher delivering through franz-go's
// idempotent producer (acks=all). Most applications wire it with Module.
type Publisher struct {
	cl *kgo.Client
}

// NewPublisher builds a Publisher on client's cluster. The connection is
// lazy: call Ping to verify it and Close to release it.
func NewPublisher(client *Client) (*Publisher, error) {
	cl, err := kgo.NewClient(client.opts()...)
	if err != nil {
		return nil, err
	}

	return &Publisher{cl: cl}, nil
}

// Ping checks that the cluster is reachable.
func (p *Publisher) Ping(ctx context.Context) error {
	return p.cl.Ping(ctx)
}

// Close flushes buffered records until ctx expires and releases the
// connection.
func (p *Publisher) Close(ctx context.Context) error {
	err := p.cl.Flush(ctx)
	p.cl.Close()

	return err
}

// Publish produces msgs and waits until the cluster acknowledged every one of
// them or ctx expires. When only part of the batch is acknowledged the rest
// is reported with a *messaging.BatchError.
func (p *Publisher) Publish(ctx context.Context, msgs ...messaging.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	for _, msg := range msgs {
		if err := msg.Validate(); err != nil {
			return err
		}
	}

	records := make([]*kgo.Record, len(msgs))
	indexes := make(map[*kgo.Record]int, len(msgs))
	for i, msg := range msgs {
		records[i] = toRecord(msg)
		indexes[records[i]] = i
	}

	failed := map[int]error{}
	for _, res := range p.cl.ProduceSync(ctx, records...) {
		if res.Err != nil {
			failed[indexes[res.Record]] = res.Err
		}
	}
	if len(failed) == 0 {
		return nil
	}

	return &messaging.BatchError{Failed: failed}
}
