# ADR-0007: Persistence: `JdbcClient` vs. JPA/Hibernate

- **Status:** Proposed. **This one is genuinely your call** (see the strongest argument against).
- **Date:** 2026-10-08
- **Deciders:** project owner

## Context

The interesting persistence code in this project is SQL-shaped, not entity-shaped:
- `pg_try_advisory_xact_lock` and ordered batch reads for the outbox relay;
- `INSERT … ON CONFLICT DO NOTHING` returning a row count for the inbox, which **must run first** in the transaction;
- `SELECT … ORDER BY sku FOR UPDATE` for deadlock-free stock locking;
- optimistic state transitions: `UPDATE … WHERE id = ? AND version = ?`, checking the row count;
- partial indexes and `CHECK` constraints owned by Flyway.

The domain model is small: orders with lines, stock rows, reservations and payments.

## Options considered

### A. Spring `JdbcClient` + Flyway (**recommended**)
Hand-written SQL in a repository class per aggregate, mapped to records.
- ✅ The SQL in the code is exactly what runs. Every locking and ordering decision is visible and reviewable.
- ✅ **Statements run when called.** The inbox insert truly happens first.
- ✅ Records map directly (JPA entities can't be records).
- ✅ No persistence context, lazy loading, dirty checking or N+1 surprises.
- ❌ More boilerplate: row mappers, and a manual insert for order + lines.

### B. Spring Data JPA (Hibernate)
- ✅ What most Java job ads list. Concise for CRUD and 1:N mappings, and `@Version` optimistic locking is built in.
- ❌ Most of the interesting queries here would be native SQL anyway.
- ❌ **Write-behind:** Hibernate defers inserts until flush. The inbox "insert first, then rely on the unique index" ordering silently becomes "insert at commit" unless you remember to `saveAndFlush`. It's exactly the kind of subtle bug this project is supposed to avoid.
- ❌ Entities must be mutable classes, not records.

### C. Spring Data JDBC
- ✅ A middle ground: aggregates with simple mapping, no persistence context, statements run immediately, and records are partially supported.
- ❌ Aggregate rules (it rewrites all child rows on save) and its own conventions to learn. Still needs `@Query` for the locking queries.

### D. jOOQ
- ✅ Type-safe SQL, excellent for SQL-heavy code.
- ❌ Code generation from the schema, plus another library to justify.

## Recommendation

**A: `JdbcClient` + Flyway.**
- One `*Repository` class per aggregate, with SQL as text blocks.
- `@Transactional` on application-service methods, never on repositories.
- Optimistic locking by checking the row count of `UPDATE … WHERE version = ?`.

## Strongest argument against

> "You're applying for Java backend roles, and nearly every posting says JPA/Hibernate. Interviewers will ask about entity mapping, lazy loading, N+1 and the persistence context. This project shows none of that. You'd also be writing row mappers and the order-lines insert by hand: boilerplate JPA eliminates, in a two-week project. Use JPA for the aggregates and native queries for the few clever bits."

That's a strong argument, and it's about your career goals rather than the design. Both answers are defensible:
- **Choose A** if you want the project's story to be "I understand exactly what SQL runs and why". In interviews, explain why you avoided JPA here, which shows you know its pitfalls (write-behind, flush ordering).
- **Choose B** if you want JPA on the résumé. Then:
  - use `saveAndFlush` (or a native insert) for the inbox row;
  - write the outbox relay and stock locking as native queries;
  - use `@Version` for the order state machine.

  That costs about half a day more for the pitfalls, about half a day less for boilerplate.

## Consequences (if A)

- Repositories return records (`Order`, `OrderLine`, `StockRow`, `Payment`). The domain logic in `domain/` is plain Java over those records.
- Every schema change is a Flyway migration. No DDL generation.
- Integration tests run against real Postgres (Testcontainers), never H2, because the SQL is Postgres-specific (`ON CONFLICT`, advisory locks, partial indexes).

## Interview soundbite

"The tricky parts of this system are SQL-level: advisory locks for the relay, insert-first inbox rows, ordered `FOR UPDATE` to avoid deadlocks, and version-checked updates. I used `JdbcClient` so the SQL in the code is exactly what runs. With Hibernate's write-behind, the inbox insert would quietly move to flush time and break the concurrency argument. I know JPA well. I chose not to use it here."
