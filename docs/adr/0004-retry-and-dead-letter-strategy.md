# ADR-0004: Retry with backoff and dead-letter topics

- **Status:** Proposed
- **Date:** 2026-10-08
- **Deciders:** project owner

## Context

A consumer can fail for two very different reasons:
- **Transient:** the database restarted, an optimistic-lock conflict, the payment simulator's "flaky" amount. Retrying will probably work.
- **Permanent (poison):** malformed JSON, an unknown `messageType`, an unsupported `schemaVersion`, a payload that fails validation. Retrying will *never* work.

With Spring Kafka's defaults, a failing record is retried a few times and then logged and skipped. That's data loss. Retrying forever blocks the partition forever. We need bounded retries for transient failures, and parking (not dropping) for poison messages.

The saga also depends on **per-order ordering**. `ReleaseInventory` must not be processed before the `ReserveInventory` for the same order.

## Options considered

### A. No retries: log and skip
- ❌ Silent data loss, and the order is stuck until it times out.

### B. Blocking retries in the consumer, then DLT (**recommended**)
`DefaultErrorHandler` + `ExponentialBackOff` retries the same record in place (the container seeks back to it). After the retries are exhausted, `DeadLetterPublishingRecoverer` publishes it to `<topic>.DLT`.
- ✅ **Keeps per-key ordering.** Nothing behind the failing record on that partition overtakes it.
- ✅ Simple: one error handler bean, no extra retry topics.
- ❌ **Head-of-line blocking.** While one record backs off, the whole partition waits.

### C. Non-blocking retry topics (`@RetryableTopic`: `topic-retry-0..n`, then `-dlt`)
- ✅ The main topic keeps flowing while failed records wait in retry topics. Better throughput under partial failure.
- ❌ **Breaks ordering.** The failed `ReserveInventory` sits in a retry topic while the `ReleaseInventory` for the same order is processed from the main topic.
- ❌ 2-4 extra topics per source topic. Harder to reason about and to test.

### D. Retry forever
- ❌ One poison message stops its partition permanently. You only notice when everything stalls.

## Recommendation

**B: bounded blocking retries, classified exceptions, and one DLT per consumed topic.**

| Setting | Value | Why |
|---|---|---|
| Backoff | 500 ms initial, ×2, max 3 retries (≈3.5 s total) | Covers short glitches (connection blips, lock conflicts) and stays far below `max.poll.interval.ms` (5 min), so a retrying consumer isn't kicked out of the group |
| Non-retryable | `MessageParseException` (bad JSON, unknown type or version), `NonRetryableMessageException` (validation) | Retrying can't fix these. Straight to the DLT, no backoff |
| Retryable | everything else, e.g. `TransientDataAccessException`, `OptimisticLockingFailureException`, `TransientPaymentException` | |
| DLT naming | `<topic>.DLT` (e.g. `inventory.commands.DLT`) | Spring Kafka convention; declared by the consuming service |
| DLT partitioning | Same partition number as the source | Requires the DLT to have ≥ the source's partition count (we use 3 = 3) |
| DLT headers | original topic/partition/offset, exception class, message, stack trace (added automatically) | Everything needed to diagnose and replay |
| Offset | Committed after the DLT publish succeeds | If the DLT publish fails, the record isn't committed and is retried, so nothing is lost |

**What happens to the business flow:** the order whose message went to the DLT stays non-terminal, so the **saga timeout** cancels it and compensates (ARCHITECTURE §7.4). The DLT holds the *message* for investigation. The *order* is resolved regardless.

**Replay:** inspect in kafka-ui, fix the cause, then re-publish the original value (same `messageId`) to the source topic with a small script. Replays are safe because consumers are idempotent and stale commands are no-ops (tombstones, expiry).

**Monitoring:** a counter for records sent to the DLT. A non-zero rate is an alert.

Producer-side failures are a separate concern. The relay simply leaves the outbox row unpublished and retries on its next run.

## Strongest argument against

> "Blocking retries mean one bad-but-retryable record stalls every other order on that partition. If the payment database is down for a minute, payment-service makes zero progress on *any* order, and all those orders time out and cancel. Non-blocking retry topics keep healthy messages flowing. That's why Uber and others built them."

Two answers:
1. **Ordering is a correctness requirement here.** Retry topics would let `ReleaseInventory` overtake `ReserveInventory` for the same order. Tombstones mitigate that, but I'd rather not rely on the mitigation.
2. When a *dependency* (the database) is down, every message fails. Retry topics just move the failures somewhere else and add load. Pausing is the right behaviour.

Non-blocking retries shine when failures are *per-message* (one bad downstream record), not *per-dependency*. With a ≈3.5 s retry budget, the head-of-line delay is bounded.

## Consequences

- One `DefaultErrorHandler` bean in `platform-messaging`, shared by all services, with the exception classification in one place.
- The JSON codec must throw `MessageParseException` (not a generic `RuntimeException`) so the classification works. Handlers throw `NonRetryableMessageException` for invalid payloads.
- Tests: a poison record goes to the DLT with headers and the next record is still processed; a transient failure ×2 is processed on the 3rd attempt; a transient failure beyond the budget goes to the DLT; the order is then cancelled by the timeout.

## Interview soundbite

"I classify failures. Poison messages go straight to a dead-letter topic with the exception in the headers. Transient ones get about three and a half seconds of exponential backoff. I chose blocking retries over retry topics on purpose: my saga needs per-order ordering, and retry topics would let a release overtake its reservation. Anything parked in the DLT doesn't leave the order hanging, because the saga timeout cancels and compensates it."
