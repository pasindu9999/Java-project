# CLAUDE.md: orderflow project conventions

An event-driven order and payments platform (portfolio project). It demonstrates an orchestrated saga, the transactional outbox, idempotent consumers, and retry + DLT, with Testcontainers failure tests.

Read first: `docs/ARCHITECTURE.md`, `docs/adr/`, `docs/PLAN.md`.

## Working agreement

- **Phase: implementation.** `docs/PLAN.md` was approved on 2026-10-08. Current day: **Day 1** (skeleton and infrastructure).
- **Never commit or push automatically.** The owner reviews and commits.
- Work follows `docs/PLAN.md` day by day. Don't start the next day's scope early.
- Every design decision must be defensible in an interview. If you change one, update the relevant ADR (or add a new one) in the same commit, including the options considered and the strongest argument against.
- Prefer the simplest approach that still demonstrates the pattern. No new dependency without a one-line justification in the PR/commit message.
- **Out of scope:** Kubernetes, authentication, frontend, tracing (stretch only, Day 13+).

## Stack

- Java 25, Spring Boot 4.1.x (manages Spring Kafka 4.1, Flyway, Jackson 3, Testcontainers 2). **Never pin a version the Boot BOM already manages.**
- Kafka 4.3 (KRaft, `apache/kafka` image), Postgres 18, Maven Wrapper, Docker Compose.
- Persistence: `JdbcClient` + Flyway (ADR-0007). No JPA, no Lombok (records replace it), no MapStruct.
- Spring Boot 4 notes: Jackson 3 lives in the `tools.jackson.*` packages. Use `org.testcontainers.kafka.KafkaContainer` (not the deprecated Confluent-based one). Check against the official Spring Boot 4 reference docs, not Boot 3 tutorials.

## Modules

```
contracts/            message records + golden-file tests. No Spring, no logic.
platform-messaging/   envelope codec, OutboxWriter/OutboxRelay, IdempotentMessageHandler,
                      Kafka error handler (auto-configuration). No domain concepts.
order-service/        REST API + saga orchestrator (port 8081)
inventory-service/    stock + reservations (port 8082)
payment-service/      payment simulator (port 8083)
e2e-tests/            all three services in one JVM against shared containers
```

Services never depend on each other. Only `e2e-tests` depends on the services.

## Package layout (per service)

Maven groupId: `io.github.pasindu9999.orderflow`. Base package: `io.github.pasindu9999.orderflow.<service>` (`order`, `inventory`, `payment`). Shared modules use `…orderflow.contracts` and `…orderflow.messaging`.

```
<service>/
  api/          REST controllers, request/response records, ProblemDetail mapping
  domain/       business rules on records/enums: pure Java, NO Spring imports
  app/          application services: @Transactional use cases (OrderSaga, ReservationService…)
  persistence/  *Repository classes (JdbcClient + SQL text blocks)
  messaging/    Kafka listeners (thin: parse, then delegate to app/), topic declarations
  config/       @Configuration and @ConfigurationProperties records
```

- Listeners and controllers stay thin. Logic lives in `app/` and `domain/`.
- `@Transactional` goes only on `app/` methods.
- **Business code never calls `KafkaTemplate`.** Messages are written with `OutboxWriter` inside the business transaction.

## Naming

| Thing | Convention | Example |
|---|---|---|
| Commands | Imperative | `ReserveInventory`, `ProcessPayment` |
| Events | Past tense | `InventoryReserved`, `PaymentFailed` |
| Topics | `<domain>.<commands\|events>`, DLT = `<topic>.DLT` | `inventory.commands.DLT` |
| Kafka key | Always `orderId` | |
| Consumer group | Service name | `inventory-service` |
| Inbox consumer name | `<topic>-handler` | `inventory-commands-handler` |
| Flyway | `db/migration/<service>/V<n>__<desc>.sql`; seed data in `db/seed/<service>` (`local` profile only) | `V2__create_orders.sql` |
| Config properties | `orderflow.<area>.*` | `orderflow.saga.timeout=PT30S` |
| Tests | Unit `*Test`, integration `*IT`; methods `should<Outcome>_when<Condition>` | `shouldSkipDuplicate_whenSameMessageIdDeliveredTwice` |

## Code style

- Records for DTOs, messages and rows. Sealed interfaces + exhaustive pattern-matching `switch` for message dispatch, with no `default` branch, so a new type is a compile error.
- Constructor injection only. Inject `Clock` (no `Instant.now()` in logic) so time-based tests are deterministic.
- Money: `BigDecimal` in code, `NUMERIC(19,2)` in the DB, decimal **string** on the wire.
- Exceptions: `MessageParseException` and `NonRetryableMessageException` go straight to the DLT. Everything else is retried (ADR-0004). Don't catch-and-swallow in listeners.
- Logging: SLF4J. `correlationId` (orderId) and `messageId` go in the MDC for every message. INFO for state transitions, WARN for ignored or out-of-state messages, ERROR only for things needing action.

## Testing rules

- **Every feature ships with an IT that covers its failure path**, not just the happy path.
- ITs use real Postgres + Kafka through Testcontainers with `@ServiceConnection`. **No H2, no embedded Kafka, no mocking repositories in ITs.**
- **Never `Thread.sleep`.** Use Awaitility (`await().atMost(10, SECONDS).untilAsserted(...)`).
- Wait for partition assignment before producing in consumer tests.
- Each test uses fresh random `orderId`s/SKUs. Never depend on another test's data or order.
- Faults are injected through the `FaultInjector` bean (a no-op in production), never through `if (test)` branches in production code.
- Singleton containers per module. Locally, enable reuse in `~/.testcontainers.properties`: `testcontainers.reuse.enable=true`.
- Done = `./mvnw verify` green locally **and** in CI.

## Run locally

Prerequisites: JDK 25 on `PATH`/`JAVA_HOME`, Docker Desktop running (≥ 6 GB RAM). Maven comes from the wrapper (3.9.16), so no global install is needed.

Pinned images (keep tests and compose in sync): `postgres:18.6-alpine`, `apache/kafka:4.3.1`, `kafbat/kafka-ui:v1.5.0`.

```bash
./mvnw verify                                   # all unit + integration tests (Windows: mvnw.cmd)
./mvnw -pl inventory-service -am verify         # one service and its dependencies

docker compose up -d                            # Kafka :9092, Postgres :5433/:5434/:5435, kafka-ui :8080
./mvnw -pl order-service -am spring-boot:run -Dspring-boot.run.profiles=local
./mvnw -pl inventory-service -am spring-boot:run -Dspring-boot.run.profiles=local
./mvnw -pl payment-service -am spring-boot:run -Dspring-boot.run.profiles=local

docker compose --profile apps up -d --build     # everything in containers (from Day 11)
docker compose down -v                          # stop and wipe volumes
```

Demo requests: `scripts/demo.http` (VS Code REST Client / IntelliJ HTTP client). Inspect topics and DLTs at http://localhost:8080.

## Git

- Conventional Commits (`feat(inventory): …`, `test(order): …`, `docs(adr): …`). Small commits; at least one push per day.
- Never commit secrets or `.env` files. Local credentials live in `docker-compose.yml` defaults only.
