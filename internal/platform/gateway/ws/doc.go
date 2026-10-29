// Package ws provides an authenticated WebSocket server with topic subscriptions
// and server-side publishing. State is in-memory and local to the process.
//
// # Protocol
//
// Every frame is a JSON text message with the [Message] envelope:
//
//	{"id":"<optional>","type":"<type>","topic":"<optional>","payload":<json>}
//
// A client must authenticate before anything else:
//
//	-> {"id":"1","type":"auth","payload":{"token":"<jwt>"}}
//	<- {"id":"1","type":"auth.ok","payload":{"subject":"alice","expiresAt":"..."}}
//
// When the token is about to expire the server sends "auth.refresh" and the
// client must answer with a new "auth" message within the refresh timeout.
//
// Subscriptions, available when a [SubscriptionHandler] is configured:
//
//	-> {"id":"2","type":"subscribe","payload":{"topic":"accounts.1"}}
//	<- {"id":"2","type":"subscribe.ok","payload":{"topic":"accounts.1"}}
//	-> {"id":"3","type":"unsubscribe","payload":{"topic":"accounts.1"}}
//	<- {"id":"3","type":"unsubscribe.ok","payload":{"topic":"accounts.1"}}
//
// Published messages always use the "event" type and carry the topic:
//
//	<- {"type":"event","topic":"accounts.1","payload":{...}}
//
// Failures are reported with the "error" type and the request id:
//
//	<- {"id":"2","type":"error","payload":{"message":"forbidden"}}
//
// Any other type is passed to the configured [Handler].
//
// # Usage
//
//	server, err := ws.NewServer(
//		ws.ServerWithLogger(log),
//		ws.ServerWithAuthenticator(auth),
//		ws.ServerWithSubscriptionHandler(handler),
//	)
//	publisher := ws.NewPublisher[BalanceUpdated](server)
//	publisher.Publish(ctx, "accounts.1", BalanceUpdated{...})
//
// With fx use [Module], [HandlerFx], [SubscriptionHandlerFx] and [PublisherFx].
//
// # Delivery
//
// Sends never block. A subscriber whose buffer is full is handled according to
// the [OverflowPolicy]; by default it is closed with status 1008.
// Subscriptions are dropped on disconnect and when a connection re-authenticates
// as a different subject. [SubscriptionHandler.OnUnsubscribe] is invoked once
// for every accepted subscription.
package ws
