# ADR-0005: Message schema format and versioning

- **Status:** Proposed
- **Date:** 2026-10-08
- **Deciders:** project owner

## Context

Services deploy independently in principle. Messages already sitting in topics, outbox tables or DLTs must still be readable after a producer or consumer changes. We need:
- a wire format;
- a way to identify the message type and version;
- rules for what counts as a compatible change;
- a way to catch accidental breaking changes before they ship.

There's also a constraint: keep dependencies minimal and explainable.

## Options considered

### A. JSON + explicit envelope, no registry (**recommended**)
Envelope: `messageId, messageType, schemaVersion, occurredAt, correlationId, causationId, producer, payload` (ARCHITECTURE §5). Payloads are Java records. Compatibility is enforced by rules and contract tests.
- ✅ Zero extra infrastructure. Human-readable in kafka-ui, in DLTs and in the outbox table (JSONB).
- ✅ The type and version are explicit *data*, not tied to Java class names.
- ❌ Nothing outside our own tests stops an incompatible producer. Larger messages than binary formats.

### B. Avro + Schema Registry (Confluent or Apicurio)
- ✅ The registry **enforces** compatibility modes (BACKWARD/FORWARD/FULL) when schemas are registered. Compact binary. The industry standard in Kafka-heavy companies.
- ❌ An extra container, Maven code-generation plugins and generated classes instead of records. Messages aren't readable without tooling.

### C. Protobuf + registry
- ✅ Same benefits as B, plus a strong cross-language story.
- ❌ Same costs as B. Its compatibility rules (field numbers) are another topic to master.

### D. Spring Kafka `JsonSerializer` with `__TypeId__` headers
- ✅ No code to write.
- ❌ Couples the wire format to **Java class names**. Renaming a package breaks consumers. The type info lives in headers that are easy to lose when replaying.

## Recommendation

**A: a JSON envelope with explicit `messageType` + `schemaVersion`, plus golden-file contract tests.**

### Compatibility rules
1. **Tolerant reader:** consumers ignore unknown fields. Set `FAIL_ON_UNKNOWN_PROPERTIES = false` explicitly, not left to library defaults, so a producer can add a field without breaking anyone.
2. **Within a version, only additive changes:** a new *optional* field (nullable, with a documented default). Never rename, remove, change a type or change meaning.
3. **A breaking change means a new version:**
   - The producer can emit v2 only after every consumer understands v1 *and* v2.
   - The consumer converts v1 → v2 in one place (an "upcaster"), so the handler logic only knows v2.
   - v1 support is removed once no v1 messages remain in topics, outboxes or DLTs.
4. **Unknown type or unsupported version is non-retryable** and goes to the DLT ([ADR-0004](0004-retry-and-dead-letter-strategy.md)). It is never silently dropped.
5. **Enums:** adding a value is a breaking change for exhaustive `switch`es, so it's treated as a version bump unless consumers map unknown values to a fallback.
6. Money is a decimal string, timestamps are ISO-8601 UTC, IDs are UUID strings.

### Contract tests (in the `contracts` module)
For every message type, a golden file `src/test/resources/contracts/<MessageType>.v<N>.json`, and three tests:
- **Serialization is stable:** serialize a sample record → must equal the golden file. Catches accidental renames and type changes.
- **Backward compatible:** deserialize the golden file → succeeds and the fields match.
- **Tolerant reader:** golden file + an extra unknown field → still deserializes.

A golden file is never edited in place. A new version gets a new file.

### Ownership
- **Commands** are defined by the receiver (inventory owns `ReserveInventory`).
- **Events** are defined by the publisher.

In this monorepo, all records live in one `contracts` module, organised by owning service (`contracts.inventory`, `contracts.payment`).

## Strongest argument against

> "Your compatibility guarantee is just discipline plus tests in one repository. In a real organisation, with independent repos and teams, a producer can deploy a breaking change and you only find out when consumers start sending everything to the DLT. A schema registry rejects the incompatible schema *before* any message is written. On top of that, a shared `contracts` module means every service compiles against the same classes, which hides exactly the drift that happens in real life."

All true. The trade-off is made knowingly:
- Minimal dependencies and readable messages matter more for this project.
- The monorepo makes the "shared classes" shortcut honest, because all the services really do change in the same commit.
- The envelope and versioning rules map directly onto registry compatibility modes (rule 2 is BACKWARD compatibility), so moving to Avro + registry changes the codec, not the design.

## Consequences

- A `contracts` module with no Spring dependency: records, sealed interfaces, and the golden-file tests.
- A `MessageCodec` in `platform-messaging`: envelope ↔ JSON (Jackson 3, Spring Boot 4's default), with dispatch on `(messageType, schemaVersion)`.
- Kafka uses `StringSerializer`/`StringDeserializer`, so the codec is the only place JSON is handled and the only source of `MessageParseException`.

## Interview soundbite

"Every message travels in an envelope with an explicit type and schema version, not a Java class name. Consumers are tolerant readers. Within a version I only allow additive optional fields, and a breaking change means a new version that consumers learn before producers emit it. Golden-file tests catch accidental breaks in CI. A schema registry would enforce that at the broker, and I'd switch to one with multiple teams. The rules are the same, so it's a codec change, not a redesign."
