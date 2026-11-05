package nats

import (
	"context"
	"errors"
	"sync"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// ErrNoAck is reported for a message the server never acknowledged before
// the publish context expired.
var ErrNoAck = errors.New("the server did not acknowledge the message in time")

// Publisher is a messaging.MessagePublisher publishing asynchronously on the
// client's shared connection and waiting for every acknowledgement, so a
// batch may span topics. It declares the stream of every topic it publishes
// to. Most applications wire it with Module.
type Publisher struct {
	client *Client

	mu      sync.Mutex
	streams map[string]struct{}
}

// NewPublisher builds a Publisher on client's connection. Open verifies the
// connection; Publish connects lazily otherwise.
func NewPublisher(client *Client) *Publisher {
	return &Publisher{client: client, streams: map[string]struct{}{}}
}

// Open connects the client if needed.
func (p *Publisher) Open(ctx context.Context) error {
	return p.client.Open(ctx)
}

// Publish sends msgs and waits until the server acknowledged every one of
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

	js, err := p.client.jetstream(ctx)
	if err != nil {
		return err
	}

	failed := map[int]error{}
	futures := map[int]jetstream.PubAckFuture{}
	for i, msg := range msgs {
		if err := p.ensureStream(ctx, js, msg.Topic); err != nil {
			failed[i] = err
			continue
		}
		future, err := js.PublishMsgAsync(toNatsMsg(msg))
		if err != nil {
			failed[i] = err
			continue
		}
		futures[i] = future
	}

	select {
	case <-js.PublishAsyncComplete():
	case <-ctx.Done():
	}

	for i, future := range futures {
		select {
		case <-future.Ok():
		case err := <-future.Err():
			failed[i] = err
		default:
			failed[i] = errors.Join(ErrNoAck, ctx.Err())
		}
	}
	if len(failed) == 0 {
		return nil
	}

	return &messaging.BatchError{Failed: failed}
}

// ensureStream declares topic's stream once per publisher.
func (p *Publisher) ensureStream(ctx context.Context, js jetstream.JetStream, topic string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.streams[topic]; ok {
		return nil
	}
	if _, err := ensureStream(ctx, js, topic); err != nil {
		return err
	}
	p.streams[topic] = struct{}{}

	return nil
}
