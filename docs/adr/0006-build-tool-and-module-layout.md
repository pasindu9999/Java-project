# ADR-0006: Build tool and module layout

- **Status:** Proposed
- **Date:** 2026-10-08
- **Deciders:** project owner

## Context

Three Spring Boot services, some shared infrastructure code (outbox, inbox, codec, error handling), message contracts and an end-to-end test suite. Java 25, Spring Boot 4.1.x. No build tool is installed globally on the dev machine. The build must be easy to explain and work the same in CI.

## Options considered

### Build tool

**Maven (with the Maven Wrapper)**
- ✅ Declarative and convention-driven. There are few ways to be clever, so builds look alike everywhere.
- ✅ The most common build tool in Java backend job ads and enterprise codebases, and the Spring Initializr default.
- ✅ `spring-boot-starter-parent` manages every dependency version (Spring Kafka, Testcontainers, Flyway, Jackson…), so we pin nothing by hand.
- ✅ The wrapper (`mvnw`) means no global install. CI and the laptop use the same Maven version.
- ❌ XML is verbose. Builds are slower (no build cache, coarser incremental compilation).

**Gradle (Kotlin DSL)**
- ✅ Faster: incremental compilation and a build cache. A flexible, type-safe build script. Spring Boot itself builds with Gradle.
- ❌ More moving parts (plugins, configuration vs. execution phase). Flexibility tempts you into custom logic that's harder to explain.
- ❌ Running on JDK 25 needs a recent Gradle version, one more compatibility matrix to manage.

### Repository and module layout

**Single repo, multi-module (recommended)**
- ✅ One PR shows a change across producer and consumer. The E2E tests can depend on all services. One CI pipeline.
- ❌ Can blur service boundaries if shared modules grow.

**One repo per service**
- ✅ Truly independent lifecycles, closer to large organisations.
- ❌ Three pipelines and published artifacts for the contracts. Heavy overhead for one developer and two weeks.

## Recommendation

**Maven multi-module in one repository, built with the Maven Wrapper.**

```
orderflow/
├── pom.xml                  parent: spring-boot-starter-parent 4.1.x, <java.version>25
├── contracts/               message records, MessageCatalog, topic names (no dependencies)
├── platform-messaging/      envelope codec + golden-file tests, outbox writer/relay, inbox handler,
│                            Kafka error handler (Spring auto-configuration)
├── order-service/
├── inventory-service/
├── payment-service/
├── e2e-tests/               boots all three services against shared containers
├── docker-compose.yml
└── docs/
```

Rules that keep the shared modules from becoming a "distributed monolith":
- `contracts` holds **data shapes only**: records, enums, sealed interfaces. No logic, no Spring.
- `platform-messaging` holds **infrastructure only**: no domain concepts, never imports from a service.
- Services never depend on each other. Only `e2e-tests` depends on all three.
- Dependencies point one way: services → `platform-messaging` → `contracts`. The golden-file tests sit in `platform-messaging` because they need the codec, and a `contracts` → `platform-messaging` test dependency would create a cycle (ADR-0005).

Build conventions:
- Surefire runs unit tests (`*Test`) during `test`. Failsafe runs integration tests (`*IT`) during `verify`.
- The Spring Boot plugin uses the `exec` classifier, so each service also produces a plain jar that `e2e-tests` can put on its classpath.

## Strongest argument against

> "Gradle's build cache and incremental compilation make a six-module build with Testcontainers ITs noticeably faster, which matters when you run it dozens of times a day for two weeks. And the shared `platform-messaging` library is a well-known microservices anti-pattern. Every service is coupled to one library version, so a change to it forces you to redeploy everything in lockstep. That's a distributed monolith."

On speed: true, but most build time here is container startup, not compilation. Testcontainers reuse locally and running modules individually (`-pl`) close most of the gap.

On the shared library: the coupling is real, which is why it's limited to infrastructure plus contracts. In a multi-repo setup it would be a versioned artifact that services upgrade on their own schedule. In a monorepo, changing in lockstep is honest.

## Consequences

- `./mvnw verify` is the single command for "everything green". CI runs exactly this.
- Each service is runnable alone: `./mvnw -pl order-service -am spring-boot:run`.
- Day 0 must install JDK 25. Maven comes from the wrapper.

## Interview soundbite

"I picked Maven because it's declarative, ubiquitous in the companies I'm applying to, and the Boot parent manages all versions, so I pinned nothing. It's a monorepo with strict rules: shared modules are infrastructure and data shapes only, services never depend on each other, and only the E2E module sees all three. I know a shared library can turn into a distributed monolith, so I deliberately kept domain logic out of it."
