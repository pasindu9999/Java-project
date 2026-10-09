# ADR-0003: Idempotent consumers

- **Status:** Proposed
- **Date:** 2026-10-08
- **Deciders:** project owner

## Context

Duplicates are guaranteed to happen, not merely possible:
- The outbox relay is at-least-once ([ADR-0002](0002-outbox-polling-vs-cdc.md)).
- A consumer that crashes after its DB commit but before its offset commit gets the record again.
- Rebalances can briefly hand the same record to two consumers.
- DLT replays re-send old messages on purpose.

Processing a message twice must have the same effect as processing it once. No double reservation, no double charge, no duplicate reply.

## Options considered

### A. Rely on Kafka exactly-once semantics (idempotent producer + transactions + `read_committed`)
- ✅ Built in.
- ❌ Covers only **Kafka → Kafka** (consume-transform-produce). Our side effects are Postgres writes, which are outside Kafka's transaction. It also doesn't help with duplicates produced by our own outbox relay.
- We still enable the idempotent producer (the default in Kafka 3+) to avoid broker-level duplicates from producer retries. It isn't the answer to application-level duplicates.

### B. Natural idempotency through business keys and state checks
For example: `reservation.order_id` is the primary key, `payment.order_id` is UNIQUE, and the state machine ignores replies that don't fit the current state.
- ✅ No extra table. It also protects against duplicates that carry *different* message IDs.
- ❌ Every handler author has to get it right, every time. Some operations have no natural key (for example, "add 5 to a counter"). Duplicates are silently swallowed, so you can't tell they happened.

### C. Inbox table: `processed_message(consumer, message_id)` in the same transaction (**recommended, together with B**)
- ✅ One generic mechanism that every handler gets for free. Duplicates are visible (log + metric).
- ✅ Atomic with the business change, so "recorded as processed" and "processed" can never disagree.
- ❌ One extra insert per message, a table that grows, and protection only for **transactional** (DB) side effects.

### D. External de-dup cache (Redis with TTL)
- ❌ Not in the same transaction as the business change. A crash between the two gives lost or duplicated effects. It also adds infrastructure.

## Recommendation

**C as the generic guarantee, with B kept as defense in depth.**

```
@Transactional
handle(envelope):
    rows = INSERT INTO processed_message(consumer, message_id) VALUES (:consumer, :messageId)
           ON CONFLICT DO NOTHING
    if rows == 0: log.info("duplicate {}", messageId); duplicates.increment(); return
    businessHandler.handle(envelope)          -- state change + outbox inserts
-- DB COMMIT, then the listener container commits the Kafka offset (AckMode.RECORD)
```

Details I can defend:
- **The inbox row goes first.** If two consumers race on the same message, the second `INSERT` blocks on the unique index until the first transaction commits, then gets the conflict. With "check, then insert at the end", both could pass the check.
- **The key includes `consumer`**, so one service can have several handlers process the same message independently.
- **The DB commit happens before the offset commit.** The opposite order would lose messages on a crash. This order gives duplicates, which the inbox absorbs.
- **Retention:** delete rows older than 14 days, which is longer than Kafka's 7-day topic retention. A message older than that can't be redelivered (except by a manual DLT replay, which runs well within that window).
- **Natural keys stay:** reservation-per-order, payment-per-order, the reply transition table and inventory's `RELEASED` tombstone.
- **At the HTTP edge**, `POST /orders` uses an `Idempotency-Key` header (unique per customer, with a request hash to catch the same key reused for a different body).

## Strongest argument against

> "It's redundant. Every handler in your system already has a natural key: one reservation per order, one payment per order, and a state machine that ignores out-of-place replies. The inbox adds a write per message and an ever-growing table, and it doesn't even solve the hard case. If payment-service called a real payment provider over HTTP, the inbox wouldn't stop a double charge, because the HTTP call isn't in your DB transaction. Only a provider-side idempotency key solves that."

The point about non-transactional side effects is correct, and it's documented as a limitation (ARCHITECTURE §10). The redundancy argument holds *today*. But the inbox:
- makes idempotency a property of the platform rather than of each handler author's diligence;
- makes duplicates observable;
- costs one indexed insert per message.

## Consequences

- `platform-messaging` provides an `IdempotentMessageHandler` template. Listeners call it and never touch `processed_message` directly.
- With JDBC (`JdbcClient`), the insert runs immediately in statement order. With JPA, the insert could be deferred until flush, which would silently break "insert first" ([ADR-0007](0007-persistence-jdbcclient-vs-jpa.md)).
- Tests: the same message delivered twice (one effect); crash after commit, before offset commit (redelivery skipped); two different message IDs for the same order (natural key holds).

## Interview soundbite

"Kafka's exactly-once stops at the Kafka boundary, and my side effects are in Postgres. So every consumer inserts the message ID into an inbox table in the same transaction as the business change: duplicate means zero rows inserted, so skip. I insert first so the unique index serializes concurrent duplicates. Natural keys like one-payment-per-order are a second line of defence. I'm explicit that this doesn't cover non-transactional calls like a real payment provider, which would need the provider's idempotency key."
