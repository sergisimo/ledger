package ws

import (
	"context"
	"errors"
)

// --------------------------------------------------------------- Contract

type (
	// SubscriptionHandler authorizes subscriptions and observes their end. Hooks run on the connection goroutine and must return promptly.
	SubscriptionHandler interface {
		OnSubscribe(ctx context.Context, conn *Conn, topic string) error
		OnUnsubscribe(ctx context.Context, conn *Conn, topic string, reason UnsubscribeReason)
	}

	// SubscriptionHandlerFuncs adapts functions to SubscriptionHandler. Nil functions allow and ignore respectively.
	SubscriptionHandlerFuncs struct {
		OnSubscribeFunc   func(ctx context.Context, conn *Conn, topic string) error
		OnUnsubscribeFunc func(ctx context.Context, conn *Conn, topic string, reason UnsubscribeReason)
	}

	// UnsubscribeReason tells OnUnsubscribe why a subscription ended.
	UnsubscribeReason string
)

const (
	// Reasons passed to OnUnsubscribe.
	UnsubscribeByClient     UnsubscribeReason = "client"
	UnsubscribeOnDisconnect UnsubscribeReason = "disconnect"
	UnsubscribeByServer     UnsubscribeReason = "server"
)

var (
	// ErrSubscriptionForbidden, returned by OnSubscribe, is reported to the client as "forbidden".
	ErrSubscriptionForbidden = errors.New("subscription is forbidden")
	// ErrUnknownTopic, returned by OnSubscribe, is reported to the client as "unknown topic". Any other error is reported as "subscription failed".
	ErrUnknownTopic = errors.New("unknown topic")
)

// --------------------------------------------------------------- Implementation

// OnSubscribe calls OnSubscribeFunc when set.
func (f SubscriptionHandlerFuncs) OnSubscribe(ctx context.Context, conn *Conn, topic string) error {
	if f.OnSubscribeFunc == nil {
		return nil
	}
	return f.OnSubscribeFunc(ctx, conn, topic)
}

// OnUnsubscribe calls OnUnsubscribeFunc when set.
func (f SubscriptionHandlerFuncs) OnUnsubscribe(ctx context.Context, conn *Conn, topic string, reason UnsubscribeReason) {
	if f.OnUnsubscribeFunc == nil {
		return
	}
	f.OnUnsubscribeFunc(ctx, conn, topic, reason)
}

// AllowAllSubscriptions accepts every topic. Intended for tests.
func AllowAllSubscriptions() SubscriptionHandler {
	return SubscriptionHandlerFuncs{}
}
