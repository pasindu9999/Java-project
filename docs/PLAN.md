# orderflow: Delivery plan (12-14 days, 6-8 h/day)

> Status: **approved 2026-10-08.** Day 0 is done, Day 1 is in progress.

## Ground rules

- **Each day ends green.** `./mvnw verify` passes locally, CI is green, work is committed and pushed, and the day's "Runnable" check works.
- **Tests come with the feature.** A feature isn't done until its failure-path IT exists.
- **Design changes go in the docs.** If reality disagrees with ARCHITECTURE.md or an ADR, update the doc in the same commit.
- **Time-box:** if a task runs more than 2 h over its estimate, stop and check the cut list.

## Milestones

| Milestone | End of day | What you can show |
|---|---|---|
| **M1: Messaging foundation** | Day 3 | `POST /orders` puts a `ReserveInventory` command on Kafka, via the outbox |
| **M2: Saga works** | Day 6 | Happy path, out-of-stock and payment-declined paths demoed with compose + `demo.http` |
| **M3: Reliability proven** | Day 9 | Retries, DLT, duplicates and crash scenarios covered by ITs |
| **M4: Shareable** | Day 12 | Timeouts, the full system in Docker, README and docs final |
| Stretch | Day 13-14 | Tracing across services, buffer |

---

## Day 0: Environment (½ day, before Day 1)

- Install **JDK 25** (Temurin). Set `JAVA_HOME` and check that `java -version` shows 25. JDK 17 is currently installed.
- Start **Docker Desktop** (WSL2 backend) and check that `docker run hello-world` works. Give Docker at least 6 GB of RAM.
- **Testcontainers spike:** a throwaway JUnit test that starts `postgres:18` and `apache/kafka:4.3.1` and connects to both. *This is the single biggest risk on Windows, so prove it works first.*
- Generate the Maven wrapper. Run `git init`, add a `.gitignore`, create an empty GitHub repo and push.
- Pick the base package (`io.github.<github-user>.orderflow`) and update CLAUDE.md.

**Done when:** the spike test passes from the command line, and the repo exists on GitHub.

## Day 1: Skeleton and infrastructure

- Parent POM (`spring-boot-starter-parent` 4.1.x, Java 25) and the modules from ADR-0006 (empty `contracts` and `platform-messaging`).
- Three services, each with Actuator, a Flyway `V1__baseline.sql` (outbox + processed_message) and `application.yml` with its port.
- `docker-compose.yml`: Kafka (KRaft, single node), 3× Postgres (5433/5434/5435), kafka-ui (8080).
- Shared test support: an `@TestConfiguration` with `@ServiceConnection` Postgres + Kafka containers.
- GitHub Actions: `./mvnw -B verify` on push.

**Runnable:** `docker compose up -d`, then all three services start and `/actuator/health` is `UP`.
**Tests:** one context-loads IT per service against real containers. CI is green.

## Day 2: Contracts and order API

- `contracts`: `Envelope`, every v1 payload record, sealed interfaces, golden files plus the three contract tests per type (ADR-0005).
- `platform-messaging`: `MessageCodec` (Jackson 3; unknown fields ignored; `MessageParseException`).
- order-service: Flyway schema for orders and order lines; `domain` (Order, status enum, transition rules as pure Java); `POST /orders` (Idempotency-Key, request hash, 202/422/400 ProblemDetail); `GET /orders/{id}`.

**Runnable:** create and read an order with curl or `demo.http` (it stays `PENDING`).
**Tests:**
- Codec unit tests: bad JSON, unknown type and unknown version.
- Transition-rule unit tests.
- API ITs: new order, same key replayed, same key with a different body (422), validation errors.

## Day 3: Outbox → M1

- `OutboxWriter` (insert inside the caller's transaction) and `OutboxRelay` (@Scheduled 200 ms, advisory lock, ordered batch, synchronous send, stop at first failure).
- Gauges: `outbox.pending` and `outbox.oldest.age`.
- `NewTopic` beans (3 partitions). order-service writes `ReserveInventory` when an order is created.

**Runnable:** create an order, then see `ReserveInventory` on `inventory.commands` in kafka-ui.
**Tests:**
- Outbox → Kafka IT: the consumed envelope equals the stored row.
- Order and outbox are atomic: when the order insert fails, no outbox row exists.
- **Relay crash after send:** inject a failure before the mark, so the message is re-sent with the same `messageId`.
- Kafka paused: rows accumulate, then drain once Kafka is unpaused (optional if slow).

## Day 4: Idempotent consumers and inventory reserve

- `platform-messaging`: `IdempotentMessageHandler` (insert-first inbox, MDC, duplicate counter); consumer factory (String deserializer, `AckMode.RECORD`, concurrency 3).
- inventory-service: schema (`product_stock`, `reservation`, `reservation_line`), seed data in the `local` profile, the reserve algorithm with sorted `FOR UPDATE`, replies via the outbox. `GET /stock/{sku}`.

**Runnable:** create an order, then inventory reserves and `InventoryReserved` appears on `inventory.events`.
**Tests:**
- Reserve succeeds (multi-line).
- Rejected when one line is short, and stock is unchanged.
- Unknown SKU.
- **Duplicate command (same messageId, twice):** one reservation, one reply.

## Day 5: Inventory release and the first half of the orchestrator

- inventory-service: `ReleaseInventory` (`RESERVED` → `RELEASED`, tombstone when there's no reservation, no-op otherwise).
- order-service: listener on `inventory.events` and an `OrderSaga` covering the transition-table rows for inventory replies, using optimistic `version` updates. Writes `ProcessPayment` (with `expiresAt`).

**Runnable:** create an order, and it reaches `AWAITING_PAYMENT` with `ProcessPayment` on `payment.commands`. An out-of-stock order goes to `CANCELLED`.
**Tests:**
- **Race for the last unit:** two orders run concurrently, exactly one is reserved, and `available` never drops below 0.
- Release is idempotent, the tombstone blocks a later reserve, and release of an unknown order works.
- Order IT with fake replies, including a duplicate `InventoryReserved` (ignored).

## Day 6: Payment and full saga → M2

- payment-service: schema, simulator rules (ARCHITECTURE §6.4), `ProcessPayment` and `RefundPayment` handlers, `GET /payments?orderId=`.
- order-service: payment-reply rows of the transition table (confirm; cancel + `ReleaseInventory`).
- `scripts/demo.http`: happy path, out of stock, declined over the limit, blocked customer.

**Runnable (milestone demo):** `docker compose up -d`, start the 3 services, run `demo.http`, and every path reaches its expected terminal state with stock restored.
**Tests:** payment rules (unit); payment-service IT for succeed, decline and duplicate command (one payment row); order IT for `PaymentFailed` (cancelled + release command written).

## Day 7: End-to-end test module

- `e2e-tests`: shared Kafka + Postgres containers (3 databases created by an init script); boot the three services in one JVM with `SpringApplicationBuilder` (separate properties and Flyway locations; services added as `exec`-classified jars).
- If it hasn't worked after 3 hours, use the **fallback**: a `scripts/smoke.sh` against compose. Log the decision.

**Runnable:** `./mvnw -pl e2e-tests verify`.
**Tests (E2E):** happy path, out of stock, declined with compensation (stock equals the initial level).

## Day 8: Retry, backoff and DLT

- A shared `DefaultErrorHandler`: `ExponentialBackOff` (500 ms ×2, 3 retries), non-retryable classification, `DeadLetterPublishingRecoverer` to `<topic>.DLT` (same partition). DLT `NewTopic`s. A DLT counter metric.
- `FaultInjector` test hook (a no-op bean in production; programmable in tests).

**Runnable:** publish garbage to `inventory.commands` by hand, see it in `inventory.commands.DLT` with headers, and the next orders still flow.
**Tests:**
- **Poison message** goes to the DLT with the right headers, and the next record is processed.
- **Transient ×2** succeeds on the 3rd attempt with one effect.
- **Transient beyond the budget** goes to the DLT.
- Payment amount `13.13` succeeds after retries.

## Day 9: Crash and at-least-once scenarios → M3

- Fault-injection points: before the DB commit, after the DB commit (`TransactionSynchronization.afterCommit`) and before the offset commit.
- order-service: the remaining out-of-state rows of the transition table (CONFIRMED + anything; CANCELLED + late `InventoryReserved` → `ReleaseInventory`).
- `correlationId`/`messageId` in the MDC and log pattern in all services.

**Runnable:** logs for a single order can be followed across the three services by `orderId`.
**Tests:**
- **Consumer crash before commit:** redelivered and processed once.
- **Consumer crash after commit:** redelivered and skipped as a duplicate (no second charge).
- Late `InventoryReserved` on a cancelled order produces `ReleaseInventory`.
- Optimistic-lock conflict is retried and resolved.

## Day 10: Saga timeout and late replies

- `OrderTimeoutSweeper` (every 5 s, batch of 50, optimistic update, `TIMEOUT` → `ReleaseInventory`).
- payment-service rejects expired commands. order-service turns a late `PaymentSucceeded` into `RefundPayment`. payment-service refunds.

**Runnable:** stop payment-service, create an order; it is `CANCELLED (TIMEOUT)` within about 35 s and stock is restored.
**Tests:**
- No inventory reply leads to a timeout.
- Payment never replies leads to a timeout and release.
- **Late `PaymentSucceeded` leads to a refund** (payment `REFUNDED`).
- An expired `ProcessPayment` is declined.
- Sweeper and reply race on the same order give one consistent terminal state.

## Day 11: Operability and containerisation

- Outbox cleanup (published rows > 7 days) and inbox cleanup (> 14 days) jobs.
- Health and metrics exposed (outbox pending/age, duplicates skipped, DLT count).
- `spring.threads.virtual.enabled=true`, with a note about HikariCP pool size.
- A Dockerfile per service (multi-stage, `eclipse-temurin:25-jre`, layered jar) and the `apps` profile in compose with healthchecks.

**Runnable:** `docker compose --profile apps up -d --build`, then `demo.http` passes against the containers.
**Tests:** the whole suite is green. Run the E2E suite 3 times in a row to check for flakiness.

## Day 12: Documentation and interview prep → M4

- README: one-paragraph pitch, architecture diagram, quick start (3 commands), the "failure scenarios we test" table linking to test classes, ADR index, and "what I'd change for production".
- Refresh ARCHITECTURE.md and the ADRs against the code. Move ADR status from Proposed to Accepted.
- Write `docs/INTERVIEW-NOTES.md`: a 2-minute walkthrough of each sequence diagram, plus likely questions and answers.
- Final pass: no TODOs, consistent naming, `./mvnw verify` clean in CI.

**Runnable:** a fresh clone, then the README's quick start works first time.

## Days 13-14: Buffer, then stretch

1. Catch up on anything slipped (the buffer comes first).
2. **Stretch: tracing.** Micrometer Tracing + OpenTelemetry exporter + Jaeger in compose. Propagate trace context through Kafka headers *and* the outbox (store `traceparent` in the envelope or row so the relay restores it). Show one order as a single trace.
3. Optional polish: a small `replay-dlt` script, Kafka lag metrics.

---

## Top risks

| # | Risk | Likelihood | Impact | Mitigation | Early warning |
|---|---|---|---|---|---|
| 1 | Docker Desktop / Testcontainers problems on Windows (WSL2, Ryuk, memory) | Medium | High | Day 0 spike; give Docker ≥ 6 GB; `testcontainers.reuse.enable=true` locally; CI on Linux as the reference | Spike not green by midday Day 0 |
| 2 | API churn from Spring Boot 4 / Spring Kafka 4 / Jackson 3 / Testcontainers 2 (most tutorials target Boot 3) | High | Medium | Use the official reference docs only; let the Boot BOM manage versions; note the differences in CLAUDE.md | Copy-pasted snippets fail to compile |
| 3 | Flaky async tests (timing, partition assignment) | High | High | Awaitility everywhere; wait for partition assignment before producing; unique IDs per test; no shared mutable state | A test fails once in 3 runs |
| 4 | E2E module classpath/config conflicts with 3 apps in one JVM | Medium | Medium | Time-box to 3 h, then use the compose smoke-script fallback | Bean or Flyway clashes |
| 5 | Scope creep (timeout/refund, tracing, polish) | Medium | High | Milestone check at the end of Days 3, 6 and 9 against this plan; follow the cut list strictly | M2 not reached by end of Day 7 |
| 6 | Slow test suite (container startup, Kafka rebalances) | Medium | Medium | Singleton containers per module; short `session.timeout` in tests; `-pl` to run one module | `verify` > 8 min |
| 7 | JDK 25 support in plugins/agents (Mockito/ByteBuddy, JaCoCo) | Low | Medium | Use Boot-managed plugin versions; skip JaCoCo if it blocks | Warnings or crashes at test start |

## What to cut if behind (in this order)

1. **Tracing.** It's already a stretch goal.
2. **Refund on late payment.** Keep the timeout sweeper and command expiry, and document the late-success race as a known gap.
3. **Containerised apps (`apps` profile).** Run the services with Maven instead. Infrastructure stays in compose.
4. **In-JVM E2E module.** Replace it with the compose smoke script. Per-service ITs remain the main proof.
5. **Cleanup jobs and custom metrics.**

**Never cut:** the outbox, the inbox/idempotency, retry + DLT, or the ITs for payment declined, duplicate delivery and consumer crash. These *are* the project.

## Milestone checkpoints

- **End of Day 3:** is M1 done? If not, drop item 5 from the cut list now.
- **End of Day 6:** is M2 done? If it slips past Day 7, drop items 3 and 4.
- **End of Day 9:** is M3 done? If not, drop item 2 and use Days 10-11 to finish M3 plus the docs.
