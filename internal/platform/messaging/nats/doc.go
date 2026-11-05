// Package nats implements the messaging contracts on NATS JetStream using the
// official nats.go client. Wire it with Module and the messaging fx helpers:
//
//	nats.Module(natsCfg)
//	messaging.PublisherFx[AccountCreated]("acme.accounts", nats.PublisherName, nats.SystemName)
//	messaging.HandlerFx(NewAccountCreatedHandler, "account-mailer")
//	nats.SubscriberFx("account-mailer", "acme.accounts", nats.SubscriberWithDeadLetterTopic("acme.accounts.dead"))
//
// # Topology
//
// A topic is a JetStream subject captured by a file-backed stream named after
// it (dots become underscores, since stream names cannot contain them), and
// the consumer name passed to SubscriberFx is a durable pull consumer on that
// stream. Instances sharing the consumer name compete for its messages,
// names each receive every message, and a durable resumes where it left off
// across restarts. Both publishers and subscribers declare the streams they
// use, so either side can start first and messages published before a
// consumer exists are retained for it.
//
// Message.ID travels as the Nats-Msg-Id header, which also lets the server
// drop duplicates republished within the stream's duplicate window;
// Message.Key has no native field and travels in messaging.HeaderMessageKey.
//
// # Semantics
//
// Delivery is at least once. A message is acknowledged only after the
// handler accepts it. A transient handler error asks the server to redeliver
// it after an exponential backoff (SubscriberWithBackoff) based on the
// delivery count, so other messages keep flowing and order is not preserved
// across retries; a handler running longer than SubscriberWithAckWait is also
// redelivered. Once the delivery count reaches SubscriberWithMaxAttempts, or
// on a Permanent (messaging.Permanent) error, the message moves to the
// SubscriberWithDeadLetterTopic stream when one is configured, stamped with
// the messaging.HeaderDeadLetterOrigin and HeaderDeadLetterReason headers;
// without one it is terminated and reported to the subscription's
// OnErrorFunc.
//
// # Connection
//
// A service shares one NATS connection, owned by the Client, over which the
// publisher and every subscription multiplex. The client reconnects
// automatically; unacknowledged messages are then redelivered.
package nats
