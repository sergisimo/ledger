// Package kafka implements the messaging contracts on Apache Kafka using
// franz-go: a MessagePublisher delivering through an idempotent producer and
// per-group Subscribers built on consumer groups. Wire it with Module and the
// messaging fx helpers:
//
//	kafka.Module(kafkaCfg)
//	messaging.PublisherFx[AccountCreated]("acme.accounts", kafka.PublisherName, kafka.SystemName)
//	messaging.HandlerFx(NewAccountCreatedHandler, "account-mailer")
//	kafka.SubscriberFx("account-mailer", "acme.accounts", kafka.SubscriberWithDeadLetterTopic("acme.accounts.dead"))
//
// # Semantics
//
// The consumer name passed to SubscriberFx (the group of NewSubscriber) is
// the Kafka consumer group: instances
// sharing it compete for the topic's partitions and messages, groups receive
// every message, and committed progress survives restarts. Message.Key is the
// partition key, so messages sharing a key keep their order; messages without
// one are spread across partitions.
//
// Delivery is at least once. Each subscription processes its records in
// order and commits (marks) a record only after the handler accepts it. A
// transient handler error is retried in process with exponential backoff —
// blocking that subscription, which is how Kafka preserves partition order —
// until it succeeds or SubscriberWithMaxAttempts is exhausted. An exhausted
// or Permanent (messaging.Permanent) error moves the message to the
// SubscriberWithDeadLetterTopic when one is configured, stamped with the
// messaging.HeaderDeadLetterOrigin and HeaderDeadLetterReason headers;
// without one the message is dropped and reported to the subscription's
// OnErrorFunc.
//
// Topics are not created by this package: they must exist, or the cluster
// must allow auto-creation and Config.AllowAutoTopicCreation be set.
package kafka
