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
//	messaging.PublisherFx[AccountCreated]("ledger.accounts", outbox.PublisherName)
//	messaging.SubscriberFx(NewAccountCreatedSubscriber, "account-mailer")
//
// Modules must be listed in start order: broker, outbox, then subscribers.
// Delivery is at least once: handlers must be idempotent and tolerate
// reordering and duplicates.
package messaging
