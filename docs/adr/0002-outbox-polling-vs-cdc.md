# ADR-0002: Transactional outbox: polling relay vs. CDC (Debezium)

- **Status:** Proposed
- **Date:** 2026-10-08
- **Deciders:** project owner

## Context

Every saga step changes the database *and* publishes a message. Postgres and Kafka can't share an atomic transaction (the "dual write" problem), so either ordering of the two operations has a failure window:

| Approach | Failure |
|---|---|
| Commit the DB, then send to Kafka | Crash between the two → state changed, message **lost**, saga stuck forever |
| Send to Kafka, then commit the DB | Commit fails → message published for a change that **never happened** |
| Send inside the DB transaction | Same as above, plus Kafka latency now holds DB locks |

The **transactional outbox** fixes this. The message is written to an `outbox` table in the *same* local transaction as the business change, and something else moves it to Kafka. The open question is *what* moves it.

## Options considered

### A. Polling relay inside each service (**recommended**)
A `@Scheduled` task (every 200 ms) reads unpublished rows in `id` order, sends each one synchronously, and marks it `published_at`.

- ✅ No extra infrastructure. About 100 lines of code whose every line I can explain.
- ✅ Works the same in tests, locally and in CI.
- ❌ Latency of up to one poll interval. Constant small queries even when there's nothing to send.
- ❌ Scaling out needs care (see "Ordering" below).

### B. Change Data Capture with Debezium
A Kafka Connect cluster with the Debezium Postgres connector tails the write-ahead log (WAL) and routes outbox rows to topics (Outbox Event Router SMT).

- ✅ Near-real-time. No polling load. Exact commit order. No relay code in the application.
- ✅ The industry-standard choice at scale.
- ❌ Adds Kafka Connect + Debezium + logical replication config: more infrastructure than the three services combined.
- ❌ Operational traps: an abandoned replication slot makes Postgres **keep WAL forever and fill the disk**. Connector offsets and snapshots also need managing.
- ❌ Harder to test failure scenarios deterministically.

### C. Kafka transactions + DB transaction ("best-effort 1PC" / chained transaction manager)
- ❌ Still not atomic. A crash between the two commits leaves them inconsistent. It narrows the window rather than closing it.

### D. "Listen to yourself" (publish first, update the DB from your own event)
- ❌ The service's own reads become eventually consistent, and the API gets more complicated. This is a different architecture (event sourcing-lite), not a fix.

## Recommendation

**A: a polling relay in each service, guarded by a Postgres advisory lock so only one instance relays at a time.**

```
BEGIN;
SELECT pg_try_advisory_xact_lock(:relayLockId);     -- false → another instance owns relaying; exit
SELECT … FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 100;
  send synchronously (get(5s)); on first failure stop the batch
  UPDATE outbox SET published_at = now() WHERE id = :id
COMMIT;
```

### Delivery guarantee
**At-least-once.** If the relay crashes after a send but before `COMMIT`, those rows go out again. That is fine because every consumer is idempotent ([ADR-0003](0003-idempotency-strategy.md)). Exactly-once *delivery* isn't achievable here. What we build is exactly-once *effect*.

### Ordering
- Messages for one order are created by successive transactions, because each step reacts to the previous reply.
- The relay sends in `id` order and **stops at the first failure**, so a later message never overtakes an earlier one.
- Kafka then preserves per-key order.

### Why `published_at IS NULL`, not a "last sent id" cursor
Identity values are assigned at **insert**, not at commit. A slow transaction holding `id = 10` can commit after `id = 11` has already been published. A high-water-mark cursor would skip row 10 forever. Polling for "not yet published" rows can't skip anything.

### Why an advisory lock instead of `FOR UPDATE SKIP LOCKED`
`SKIP LOCKED` lets several instances relay *different* rows in parallel. That's good for throughput, but two messages for the same order could then go out of order. With one relay at a time, ordering is trivially correct. Scaling out later would mean partitioning the relay by `hash(message_key) % N`, with one lock per partition.

### Housekeeping
- Delete published rows older than 7 days (hourly job).
- Expose `outbox.pending` (count) and `outbox.oldest.age` gauges. A growing age is the alert signal that Kafka is down or the relay is stuck.

## Strongest argument against

> "Polling is a toy. You're querying the database five times a second per service even when idle, adding up to 200 ms of latency per saga step (≈600 ms per order), and you've capped throughput at one relay instance. Debezium reads the WAL: zero polling, commit-ordered, horizontally scalable, and no relay code to get wrong. It's what production systems actually use."

All true, and it's the right call at scale. For this project the counter-arguments are:
- Debezium's operational surface (Connect cluster, replication slots, WAL retention) outweighs the whole application.
- The polling relay makes the *pattern* explicit and testable.
- The outbox table schema is the same either way. Moving to Debezium later means deleting the relay and adding a connector config, with no change to business code.

## Consequences

- `platform-messaging` provides `OutboxWriter` (called inside business transactions) and `OutboxRelay` (scheduled).
- Each service needs a `relayLockId` constant. All Kafka sends go through the outbox, and **business code never calls `KafkaTemplate` directly**.
- Tests: relay crash after send → duplicate is sent → consumer skips it. Kafka unavailable → rows accumulate → drained when Kafka returns.

## Interview soundbite

"You can't atomically commit to Postgres and Kafka, so I write the message into an outbox table in the same transaction as the state change and relay it afterwards. That gives at-least-once delivery, and idempotent consumers turn that into exactly-once effect. I chose polling over Debezium because it's a hundred lines I fully control, and the table design is CDC-ready if latency ever matters. One subtle point: I select by 'unpublished' rather than a last-id cursor, because identity values are allocated before commit and a cursor can skip rows."
