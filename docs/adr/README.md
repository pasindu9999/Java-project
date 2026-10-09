# Architecture Decision Records

Each ADR lists the options considered, a recommendation, **the strongest argument against it**, and an interview soundbite. All start as *Proposed*. The owner makes the final call and changes the status to *Accepted*.

| ADR | Decision | Recommendation |
|---|---|---|
| [0001](0001-saga-orchestration-vs-choreography.md) | Saga coordination | Orchestration inside order-service |
| [0002](0002-outbox-polling-vs-cdc.md) | Outbox publishing | Polling relay with an advisory lock (not Debezium) |
| [0003](0003-idempotency-strategy.md) | Idempotent consumers | Inbox table in the same transaction, plus natural keys |
| [0004](0004-retry-and-dead-letter-strategy.md) | Retry and dead-letter | Bounded blocking retries, classified exceptions, `<topic>.DLT` |
| [0005](0005-event-schema-and-versioning.md) | Schema and versioning | JSON envelope with `messageType` + `schemaVersion`, golden-file tests |
| [0006](0006-build-tool-and-module-layout.md) | Build and modules | Maven multi-module monorepo with wrapper |
| [0007](0007-persistence-jdbcclient-vs-jpa.md) | Persistence | `JdbcClient` + Flyway (genuinely your call vs. JPA) |

New decisions: copy the structure of any ADR above and use the next number.
