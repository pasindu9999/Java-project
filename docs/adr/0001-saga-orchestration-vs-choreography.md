# ADR-0001: Saga coordination: orchestration vs. choreography

- **Status:** Proposed
- **Date:** 2026-10-08
- **Deciders:** project owner

## Context

Placing an order spans three services: reserve inventory, charge payment, confirm the order. The services can't share a database transaction (each owns its own database), so we need a **saga**: a sequence of local transactions, where each completed step has a compensating action that runs if a later step fails.

The saga must also handle:
- a participant that never replies (timeout);
- a reply that arrives after we've given up (late reply);
- duplicate replies.

Someone has to own "where is this order in the process?"

## Options considered

### A. Choreography (event-driven, no coordinator)
Each service reacts to the others' domain events. `OrderCreated` → inventory reserves → `InventoryReserved` → payment charges → `PaymentSucceeded` → order confirms. On `PaymentFailed`, inventory releases *and* order cancels.

- ✅ No central component. Services are loosely coupled, and events are reusable by future consumers (notifications, analytics).
- ✅ Less code for a short, linear flow.
- ❌ The flow exists only implicitly, spread across three codebases. To answer "what happens after X?" you have to read all of them.
- ❌ **Timeouts have no natural owner.** Who notices that payment never answered?
- ❌ Participants learn about each other. payment-service must know that `InventoryReserved` means "charge now", which leaks the flow into every service.
- ❌ Adding or reordering a step touches several services. Cyclic event dependencies become likely.

### B. Orchestration inside order-service (**recommended**)
order-service runs a state machine. It sends **commands** (`ReserveInventory`, `ProcessPayment`, `ReleaseInventory`, `RefundPayment`) and reacts to **replies**. Saga state is the order's `status` + `deadline_at` + `version`.

- ✅ The whole flow, including every compensation, is **one transition table** in one class. It can be reviewed and tested in isolation (ARCHITECTURE §3.3).
- ✅ The state can be queried (`GET /orders/{id}` shows where the saga is).
- ✅ Timeouts and late replies have an obvious owner.
- ✅ Participants stay ignorant: inventory-service doesn't know payments exist.
- ❌ order-service is coupled to every participant's command API.
- ❌ Risk of a "god service" if business logic creeps into the orchestrator.

### C. Workflow engine (Temporal, Camunda, Conductor)
- ✅ Durable timers, retries, versioning and visibility out of the box. This is what many companies use in production.
- ❌ Adds a heavy runtime and SDK whose internals (event history, deterministic replay) I couldn't defend in depth. It also hides the very patterns this project exists to demonstrate.

### D. Separate orchestrator service
Same as B, but in its own deployable.
- ❌ An extra service with no domain of its own. The saga is *about* an order, so the order service is its natural home.

## Recommendation

**B: orchestration embedded in order-service**, with the state machine on the `orders` row.

Payment is placed as the **pivot** step: compensatable steps (inventory) come before it, and a step that can't fail (confirm) comes after it. In the normal decline path, nothing that has already happened needs a refund.

## Strongest argument against

> "For three participants and a straight-line flow, orchestration is over-engineering. Choreography needs less code and no coordinator, and its events (`InventoryReserved`, `PaymentSucceeded`) are facts any future service can subscribe to. Your commands are point-to-point RPC in disguise. And order-service now has to know the command API of every other service, which is exactly the coupling microservices are supposed to avoid."

That criticism is fair for the happy path. It breaks down on the failure paths this project is about:
- With choreography, *someone* still has to own "this order has been stuck for 30 s".
- Someone has to decide that a `PaymentSucceeded` arriving after cancellation needs a refund.

Whoever takes on those jobs becomes a de facto orchestrator anyway.

On coupling: participants define their own command schemas, and order-service only depends on those published contracts.

## Consequences

- order-service contains the `OrderSaga` (transition table), `OrderTimeoutSweeper` and the reply listeners. Business rules stay in inventory and payment. The orchestrator only sequences.
- Every `(status, message)` pair is explicitly handled or explicitly ignored, and the unusual ones have tests.
- `ProcessPayment` carries `expiresAt`, so participants can refuse stale work. That reduces the cases needing late-reply compensation.
- If more sagas appear, saga state would move to a dedicated `saga_instance` table rather than living on the business entity.

## Interview soundbite

"I used orchestration because the hard part of a saga isn't the happy path. It's timeouts, late replies and compensation, and each of those needs a single owner. The whole flow is one transition table I can unit-test. Payment is the pivot step, so a decline only ever has to undo a reservation, never a charge."
