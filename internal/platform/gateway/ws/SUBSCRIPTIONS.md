# Topic subscriptions and message routing for `ws`

Design proposal for adding topic subscriptions, subscribe/unsubscribe hooks and
server-side publishing to `internal/platform/gateway/ws`.

## 1. Goals and constraints

- A connected, authenticated client can subscribe to and unsubscribe from topics.
- The library user can publish a message to a topic and every subscribed
  connection on this node receives it.
- The library user can hook into subscribe and unsubscribe to authorize the
  request (and to release resources on unsubscribe/disconnect).
- State is in-memory and per node. Connections die with the process, so the
  subscription table can die with it too.
- No distributed fan-out. Each service bridges its external pub/sub into
  `Publish`; the `ws` package only routes to local connections.

Non-goals for the first iteration: wildcard topics, replay/backlog, delivery
acknowledgements, cross-node fan-out.

## 2. What exists today (relevant pieces)

- `Server` owns `conns map[ConnID]*Conn` under `s.mu`, and `register` /
  `unregister` bracket every connection's lifetime (`server.go`).
- `Conn.dispatch` already intercepts a built-in protocol type (`auth`) before
  delegating to the user `Handler`, and enforces "authenticated" for everything
  else (`conn.go`). Subscriptions fit the same slot.
- `Conn.Send` is non-blocking: it returns `ErrSendBufferFull` instead of
  waiting. Fan-out must decide what to do with slow consumers.
- Every outgoing `Message` is JSON-marshalled in the per-connection
  `writePump`. A fan-out of one event to N connections currently costs N
  marshals.
- `wstest` offers a real server + client helpers, so routing can be tested
  end-to-end.

## 3. Wire protocol (common to all architectures)

The envelope stays `{"id","type","payload"}` and gains an optional `topic`
field, so clients keep dispatching on `type` and treat `topic` as routing
metadata. Adding an `omitempty` field is backwards compatible.

Client to server:

```json
{"id":"7","type":"subscribe","payload":{"topic":"accounts.123"}}
{"id":"8","type":"unsubscribe","payload":{"topic":"accounts.123"}}
```

Server to client:

```json
{"id":"7","type":"subscribe.ok","payload":{"topic":"accounts.123"}}
{"id":"8","type":"unsubscribe.ok","payload":{"topic":"accounts.123"}}
{"id":"7","type":"error","payload":{"message":"forbidden"}}
{"type":"balance.updated","topic":"accounts.123","payload":{"balance":"10.00"}}
```

Rules:

- `subscribe` / `unsubscribe` require the authenticated phase, which
  `dispatch` already enforces for every non-`auth` type.
- Both are idempotent. Subscribing twice is a single subscription and the
  hook runs once. Unsubscribing an unknown topic answers `unsubscribe.ok`.
- A published event carries the publisher's `type` and the `topic`. The
  server never wraps the payload.
- Topics are opaque, exact-match strings. Validation: non-empty, max length
  (e.g. 256 bytes). Naming conventions (`accounts.<id>`) are a service
  concern.

Alternative considered: wrapping events as
`{"type":"event","payload":{"topic":...,"event":...,"data":...}}`. Rejected:
it forces a second level of dispatch in every client and nests the payload.

## 4. Architecture options

### Option A: Built into `Server`

`Server` owns the subscription table and the protocol handling. `Conn.dispatch`
intercepts `subscribe` / `unsubscribe` exactly like it intercepts `auth`.
`Server.unregister` drops the connection's subscriptions. `Server.Publish`
fans out. Hooks are configured with `ServerWithSubscriptionHandler(...)`.

Pros

- Zero wiring for library users; mirrors how authentication is already
  built in.
- Disconnect cleanup is trivial and race-free: it happens in the same
  critical section that removes the connection.
- Publish and the connection table live behind the same object, so shutdown
  semantics (`beginShutdown` sets `conns = nil`) extend naturally.

Cons

- `server.go` and `conn.go` grow. Without discipline the registry logic
  gets tangled with the transport logic.
- The feature is always compiled in, even for services that never publish.

### Option B: Separate `Hub` composed with `Server` via a handler decorator

A `ws.Hub` type keeps the table and exposes `Publish`. Users wrap their handler:
`ServerWithHandler(hub.Handler(myHandler))`. The hub intercepts the two
subscription types and forwards the rest. Cleanup on disconnect needs a signal
from the server: either a new `ServerWithOnDisconnect` option or a goroutine per
connection waiting on `conn.Context().Done()`.

Pros

- Strong separation. The hub is testable without a socket.
- Opt-in by construction.

Cons

- The server still needs a new disconnect hook, so the package changes anyway.
- A goroutine per connection for cleanup is wasteful; the hook variant leaks
  hub concerns into `Server` options.
- Two objects to wire with fx (`Hub` and the decorated handler), and a subtle
  race: a `subscribe` processed after the disconnect hook fired leaves a dead
  entry unless the hub re-checks `conn.Context().Err()` under its lock.

### Option C: Transport-agnostic `pubsub` sub-package

`ws/pubsub` defines `Subscriber interface { ID(); Send(Message) error }` and a
generic registry. The `ws` package adds the hooks needed to drive it. The same
registry could serve SSE or gRPC streams later.

Pros

- Maximum reuse and isolation.

Cons

- There is no second transport today. The interfaces are speculation.
- Same disconnect-hook and race issues as Option B, plus more surface area.

### Option D: Actor per topic

Each topic is a goroutine owning its subscriber set, fed by a channel of
`subscribe` / `unsubscribe` / `publish` commands. No shared mutex.

Pros

- No lock contention between topics.

Cons

- Goroutine churn for short-lived topics, lifecycle of idle topics, ordering
  across topics, and shutdown all become explicit problems.
- Publish becomes asynchronous, so the caller cannot know delivery count or
  that the server is shutting down.
- At the expected scale (one node, in-memory, non-blocking sends) a `RWMutex`
  around two maps is not a bottleneck.

## 5. Recommendation

**Option A for wiring, with the registry isolated in its own file and type**
(`subscriptions.go`, unexported `subscriptions` struct with its own mutex).
This gives the user experience of Option A and the testability of Option B,
without adding public hooks that only exist to glue two objects together.

Concretely:

```go
// Hooks. Returning an error from OnSubscribe rejects the subscription.
type SubscriptionHandler interface {
    OnSubscribe(ctx context.Context, conn *Conn, topic string) error
    OnUnsubscribe(ctx context.Context, conn *Conn, topic string, reason UnsubscribeReason)
}

type UnsubscribeReason int // UnsubscribeByClient, UnsubscribeOnDisconnect, UnsubscribeByServer

// Configuration.
func ServerWithSubscriptionHandler(h SubscriptionHandler) ServerOpt
func ServerWithMaxSubscriptions(perConn int) ServerOpt   // default 128
func ServerWithMaxTopicLength(n int) ServerOpt           // default 256

// Publishing. Returns the number of connections the message was queued for.
func (s *Server) Publish(ctx context.Context, topic, msgType string, v any) (int, error)

// Introspection / server-side revocation (useful when permissions change).
func (c *Conn) Subscriptions() []string
func (s *Server) Unsubscribe(connID ConnID, topic string) bool
```

Decisions baked into the recommendation:

1. **Opt-in, default deny.** If no `SubscriptionHandler` is configured,
   `subscribe` answers `error: "subscriptions are not enabled"`. A ledger
   must not expose topics by accident. Provide `AllowAllSubscriptions()` for
   tests and local development.
2. **Error mapping.** `OnSubscribe` errors are mapped like today: sentinel
   `ErrSubscriptionForbidden` and `ErrUnknownTopic` are sent verbatim
   (`"forbidden"`, `"unknown topic"`). Any other error is logged and reported
   as `"subscription failed"` so internals do not leak to clients.
3. **Slow consumers.** When `Publish` hits `ErrSendBufferFull` the connection
   is closed with status 1008 and reason `"slow consumer"`. A silent gap is
   worse than a reconnect for a client mirroring ledger state. This is
   configurable (`ServerWithOverflowPolicy(OverflowClose | OverflowDrop)`),
   and services with heavy fan-out should raise `ServerWithSendBuffer`.
4. **Hook ordering and locking.** Hooks run outside the registry lock.
   Flow for `subscribe`: validate topic, check limits, call `OnSubscribe`,
   then insert under lock while re-checking `conn.ctx.Err()`. If the
   connection died in between, call `OnUnsubscribe(..., OnDisconnect)` so the
   handler sees a balanced pair.
5. **Disconnect cleanup.** `serveConn`'s deferred `unregister` also removes
   the connection's topics and invokes `OnUnsubscribe` with
   `UnsubscribeOnDisconnect` using `context.WithoutCancel(conn.ctx)`, since
   the connection context is already cancelled.
6. **Marshal once.** `Publish` marshals the envelope a single time and pushes
   the encoded bytes to each connection. This requires the send channel to
   carry a small `frame` (encoded bytes) instead of `Message`; `Send(Message)`
   keeps its signature and encodes before enqueueing. This also moves the
   "unencodable message" error from the write pump to the caller, which is
   an improvement on its own.
7. **Shutdown.** After `beginShutdown`, `Publish` returns `ErrServerClosed`
   (new sentinel) and `0`. The registry is cleared together with `conns`.
8. **Token refresh.** Subscriptions survive a successful refresh. If the
   refreshed identity has a different `Subject`, the server drops all
   subscriptions for that connection (`UnsubscribeByServer`) and the client
   must resubscribe. Cheap to implement and avoids authorization drift.
9. **fx.** `SubscriptionHandlerFx(constructor)` mirrors `HandlerFx`. Also
   provide a `Publisher` interface (`Publish` only) satisfied by `*Server`, so
   business modules depend on the interface rather than the server.

## 6. Implementation steps

Each step is independently reviewable and keeps `go test -race ./...` green.

1. **Protocol types** (`message.go`)
   Add `Topic string json:"topic,omitempty"` to `Message`. Add constants
   `TypeSubscribe`, `TypeSubscribeOK`, `TypeUnsubscribe`, `TypeUnsubscribeOK`
   and `SubscribePayload{Topic}`. Add `NewTopicMessage(topic, msgType, v)`.

2. **Encoded frames in the send path** (`conn.go`)
   Change `send chan Message` to `send chan []byte`. `Send` marshals and
   returns the encoding error. `write` just writes bytes. Add an unexported
   `sendFrame([]byte) error` used by `Publish`. Update tests that rely on the
   dropped-message log.

3. **Registry** (`subscriptions.go`, `subscriptions_test.go`)
   Unexported `subscriptions` with `sync.RWMutex`,
   `byTopic map[string]map[ConnID]*Conn` and `byConn map[ConnID]map[string]struct{}`.
   Methods: `add(conn, topic) (added bool)`, `remove(conn, topic) bool`,
   `removeConn(conn) []string`, `snapshot(topic) []*Conn`, `count(conn) int`,
   `clear()`. Pure unit tests, including concurrency with `-race`.

4. **Hooks and options** (`subscription_handler.go`, `server.go`)
   `SubscriptionHandler`, `SubscriptionHandlerFunc` helpers,
   `UnsubscribeReason`, sentinel errors, `AllowAllSubscriptions()`,
   `ServerWithSubscriptionHandler`, `ServerWithMaxSubscriptions`,
   `ServerWithMaxTopicLength`, `ServerWithOverflowPolicy`, env defaults
   (`WS_MAX_SUBSCRIPTIONS`, `WS_MAX_TOPIC_LENGTH`).

5. **Protocol handling** (`conn.go`)
   In `dispatch`, route `TypeSubscribe` / `TypeUnsubscribe` to
   `conn.subscribe` / `conn.unsubscribe` before the user handler. Implement
   validation, limit checks, hook invocation, error mapping and the
   `*.ok` replies. Add `Conn.Subscriptions()`.

6. **Disconnect and refresh cleanup** (`server.go`, `conn.go`)
   Extend `unregister` to call `subscriptions.removeConn` and fire
   `OnUnsubscribe(..., UnsubscribeOnDisconnect)` for each topic. In
   `authenticate`, when the phase was `phaseRefreshing` and the subject
   changed, drop subscriptions with `UnsubscribeByServer`.

7. **Publishing** (`publish.go`)
   `Server.Publish` and `Server.Unsubscribe`. Marshal once, snapshot
   subscribers, `sendFrame` to each, apply overflow policy, return delivered
   count. Return `ErrServerClosed` after shutdown began.

8. **fx wiring** (`module.go`)
   `SubscriptionHandlerFx(constructor)` and `Publisher` interface provided
   from `*Server`.

9. **Test helpers** (`wstest/server.go`)
   `Subscribe(t, conn, topic)`, `Unsubscribe(t, conn, topic)`,
   `ReceiveTopic(t, conn, topic)` and a recording `SubscriptionHandler` that
   captures hook calls for assertions.

10. **Integration tests** (`server_test.go` or `subscriptions_server_test.go`)
    Cover: subscribe then publish delivers; unsubscribed connection does not
    receive; unauthenticated subscribe rejected; handler rejection maps to
    the right error text; idempotent subscribe calls the hook once; per-conn
    limit; topic validation; disconnect fires `OnUnsubscribe` for every
    topic; slow consumer closed with 1008; publish during shutdown returns
    `ErrServerClosed`; subject change on refresh drops subscriptions;
    `Publish` concurrent with subscribe/unsubscribe under `-race`.

11. **Documentation**
    Add a "Subscriptions" section to the package docs / README with the
    protocol above and a short example of a handler that authorizes
    `accounts.<id>` against `conn.Identity()`.

12. **Follow-ups (not in this iteration)**
    Wildcard or prefix topics, per-topic metrics (subscriber count, drops,
    slow-consumer closes), and a bridge helper that adapts an external
    pub/sub subscription into `Publisher`.

## 7. Open questions for you

- Slow-consumer default: close (recommended) or drop? See decision 3.
- Should `OnUnsubscribe` be invoked on disconnect at all? Recommended yes,
  with the reason, so handlers can release per-subscription resources.
- Default for `ServerWithMaxSubscriptions`: 128 is a guess; adjust to the
  expected number of accounts a single client follows.
