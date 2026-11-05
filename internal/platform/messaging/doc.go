// Package messaging provides broker-agnostic, event-based communication for
// platform services: a Message envelope, typed publishers, per-topic
// subscribers and fx helpers to wire them.
//
// Use cases depend only on Publisher[T] and on handlers built with Handle;
// the broker technology (inmem, ...) and the delivery guarantee (direct or
// transactional outbox) are fx wiring decisions:
//
//	inmem.Module(shutdownTimeout)
//	outbox.Module(outboxCfg, inmem.PublisherName)
//	outboxsql.StoreFx()
//	messaging.PublisherFx[AccountCreated]("acme.accounts", outbox.PublisherName, inmem.SystemName)
//	messaging.HandlerFx(NewAccountCreatedHandler, "account-mailer")
//	inmem.SubscriberFx("account-mailer", "acme.accounts")
//
// Modules must be listed in start order: broker, outbox, then subscribers.
//
// # Delivery semantics
//
// Delivery is at least once: handlers must be idempotent and tolerate
// reordering and duplicates. A handler acknowledges its message by returning
// nil. An error marked Permanent (as Handle does for undecodable payloads)
// tells the broker no redelivery can succeed: the message is rejected for
// good and dead-lettered where the broker is configured to. Any other error
// asks for redelivery where the broker supports it.
//
// The consumer name passed to SubscriberFx (and to the broker packages'
// constructors) is the subscription's durable identity: on networked brokers,
// instances sharing a name form one group and compete for the topic's
// messages, while subscriptions with different names each receive every
// message. The in-memory broker has no shared identity: every subscription
// instance receives every message.
//
// Message.Key is a partitioning and ordering hint: brokers with the concept
// (e.g. Kafka partition keys, RabbitMQ routing keys) honour it and the others
// ignore it. Cross-broker metadata travels in Message.Headers; see the Header
// constants.
package messaging
