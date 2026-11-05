// Package redis implements the messaging contracts on Redis Streams (Redis
// 7+ or Valkey) using go-redis. Wire it with Module and the messaging fx
// helpers:
//
//	redis.Module(redisCfg)
//	messaging.PublisherFx[AccountCreated]("acme.accounts", redis.PublisherName, redis.SystemName)
//	messaging.HandlerFx(NewAccountCreatedHandler, "account-mailer")
//	redis.SubscriberFx("account-mailer", "acme.accounts", redis.SubscriberWithDeadLetterTopic("acme.accounts.dead"))
//
// # Topology
//
// A topic is a stream keyed by its name and the consumer name passed to
// SubscriberFx is a consumer group on it, created from the start of the
// stream. Instances sharing the consumer name compete for its entries (each
// Subscribe joins as a uniquely named consumer), names each receive every
// entry, and a group resumes where it left off across restarts. Streams are
// created on first publish and retained up to Config.StreamMaxLen entries,
// so messages published before a consumer exists are kept for it.
//
// Message fields map to entry fields: the payload, ID and key plus one
// "h:"-prefixed field per header. Message.Key has no routing role: Redis
// Streams do not partition.
//
// # Semantics
//
// Delivery is at least once. An entry is acknowledged (XACK) only after the
// handler accepts it. A transient handler error is retried in process with
// exponential backoff — blocking that subscription — until it succeeds or
// SubscriberWithMaxAttempts is exhausted. An exhausted or Permanent
// (messaging.Permanent) error moves the message to the
// SubscriberWithDeadLetterTopic stream when one is configured, stamped with
// the messaging.HeaderDeadLetterOrigin and HeaderDeadLetterReason headers;
// without one the message is dropped and reported to the subscription's
// OnErrorFunc.
//
// Entries left pending by a crashed or stalled instance are reclaimed
// (XAUTOCLAIM) by the group's live instances once idle for
// SubscriberWithClaimIdle, so keep it above the slowest handler including its
// retries, or a slow message is processed twice.
//
// # Connection
//
// A service shares one go-redis client (a connection pool), owned by the
// Client, over which the publisher and every subscription operate.
package redis
