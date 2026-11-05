// Package rabbitmq implements the messaging contracts on RabbitMQ 4.x over
// AMQP 1.0 using the official rabbitmq-amqp-go-client. Wire it with Module
// and the messaging fx helpers:
//
//	rabbitmq.Module(rabbitCfg)
//	messaging.PublisherFx[AccountCreated]("acme.accounts", rabbitmq.PublisherName, rabbitmq.SystemName)
//	messaging.HandlerFx(NewAccountCreatedHandler, "account-mailer")
//	rabbitmq.SubscriberFx("account-mailer", "acme.accounts", rabbitmq.SubscriberWithDeadLetterTopic("acme.accounts.dead"))
//
// # Topology
//
// Unlike the kafka package, topology is declared by this package: a topic is
// a durable fanout exchange named after it, and the consumer name passed to
// SubscriberFx owns the durable quorum queue "<topic>.<consumerName>" bound
// to that exchange. Instances sharing the consumer name compete for the
// queue's messages, names each receive every message, and unconsumed
// messages wait in the queue across restarts. Publishers declare the
// exchanges they publish to, so either side can start first; a message
// published to a topic whose consumers never subscribed yet has no queue to
// land in and is dropped (delivered to zero subscriptions, like the
// in-memory broker) — publish through the outbox when that loss matters.
//
// Message.Key travels as the AMQP subject (the routing key). The fanout
// exchanges ignore it for routing, so unlike Kafka it does not order a
// partition; it stays available as metadata.
//
// # Semantics
//
// Delivery is at least once. Each subscription processes its queue's
// messages in order and settles a message only after the handler accepts it.
// A transient handler error is retried in process with exponential backoff —
// blocking that subscription — until it succeeds or SubscriberWithMaxAttempts
// is exhausted. An exhausted or Permanent (messaging.Permanent) error moves
// the message to the SubscriberWithDeadLetterTopic exchange when one is
// configured, stamped with the messaging.HeaderDeadLetterOrigin and
// HeaderDeadLetterReason headers; the package also binds a catch-all queue
// named after the dead-letter topic so dead letters survive without a
// dead-letter consumer. Without a dead-letter topic the message is discarded
// and reported to the subscription's OnErrorFunc.
//
// # Connection
//
// A service shares one AMQP connection, owned by the Client: AMQP 1.0
// multiplexes the publisher and every subscription over it on their own
// links, so adding consumers costs no connections. The client reconnects and
// recovers its links automatically after connection failures; unsettled
// messages are then redelivered.
package rabbitmq
