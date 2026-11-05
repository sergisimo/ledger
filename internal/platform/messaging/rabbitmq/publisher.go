package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"

	"github.com/sergisimo/ledger/internal/platform/messaging"
)

// ErrPublisherClosed is returned by Publish before Open or after Close.
var ErrPublisherClosed = errors.New("rabbitmq publisher is not open")

// Publisher is a messaging.MessagePublisher delivering through one anonymous
// AMQP 1.0 sender on the client's shared connection, so a batch may span
// topics. It declares the fanout exchange of every topic it publishes to.
// Most applications wire it with Module, which opens it on start.
type Publisher struct {
	client *Client

	mu        sync.Mutex
	conn      *rabbitmqamqp.AmqpConnection
	pub       *rabbitmqamqp.Publisher
	exchanges map[string]struct{}
}

// NewPublisher builds a Publisher on client's connection. Call Open before
// publishing and Close to detach the sender.
func NewPublisher(client *Client) *Publisher {
	return &Publisher{client: client, exchanges: map[string]struct{}{}}
}

// Open attaches the sender to the client's connection, dialing it if needed.
// It is a no-op on an open publisher.
func (p *Publisher) Open(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pub != nil {
		return nil
	}

	conn, err := p.client.connection(ctx)
	if err != nil {
		return err
	}

	pub, err := conn.NewPublisher(ctx, nil, nil)
	if err != nil {
		return err
	}

	p.conn, p.pub = conn, pub

	return nil
}

// Close detaches the sender; the connection stays open for the client's
// other users. It is a no-op on a closed publisher.
func (p *Publisher) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pub == nil {
		return nil
	}

	err := p.pub.Close(ctx)
	p.pub, p.conn = nil, nil
	p.exchanges = map[string]struct{}{}

	return err
}

// Publish sends msgs and waits for the broker's outcome of every one of
// them. A message routed to no queue counts as delivered to zero
// subscriptions, like the in-memory broker. When only part of the batch is
// accepted the rest is reported with a *messaging.BatchError.
func (p *Publisher) Publish(ctx context.Context, msgs ...messaging.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	for _, msg := range msgs {
		if err := msg.Validate(); err != nil {
			return err
		}
	}

	p.mu.Lock()
	pub := p.pub
	p.mu.Unlock()
	if pub == nil {
		return ErrPublisherClosed
	}

	failed := map[int]error{}
	for i, msg := range msgs {
		if err := p.publish(ctx, pub, msg); err != nil {
			failed[i] = err
		}
	}
	if len(failed) == 0 {
		return nil
	}

	return &messaging.BatchError{Failed: failed}
}

func (p *Publisher) publish(ctx context.Context, pub *rabbitmqamqp.Publisher, msg messaging.Message) error {
	if err := p.ensureExchange(ctx, msg.Topic); err != nil {
		return err
	}

	out, err := toAmqpMessage(msg)
	if err != nil {
		return err
	}

	res, err := pub.Publish(ctx, out)
	if err != nil {
		return err
	}

	switch outcome := res.Outcome.(type) {
	case *rabbitmqamqp.StateAccepted:
		return nil
	case *rabbitmqamqp.StateReleased:
		p.client.log.
			With("topic", msg.Topic).
			With("msgID", msg.ID).
			Warn(ctx, "message was routed to no queue and dropped; no consumer has subscribed to the topic yet")
		return nil
	case *rabbitmqamqp.StateRejected:
		if res.MessageRejectedError != nil {
			return res.MessageRejectedError
		}
		if outcome.Error != nil {
			return outcome.Error
		}
		return fmt.Errorf("message %s on topic %s was rejected by the broker", msg.ID, msg.Topic)
	default:
		return fmt.Errorf("message %s on topic %s got unexpected outcome %T", msg.ID, msg.Topic, res.Outcome)
	}
}

// ensureExchange declares topic's fanout exchange once per publisher.
func (p *Publisher) ensureExchange(ctx context.Context, topic string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.exchanges[topic]; ok {
		return nil
	}
	if p.conn == nil {
		return ErrPublisherClosed
	}

	if _, err := p.conn.Management().DeclareExchange(ctx,
		&rabbitmqamqp.FanOutExchangeSpecification{Name: topic}); err != nil {
		return err
	}
	p.exchanges[topic] = struct{}{}

	return nil
}
