/**
 * Message contracts (envelope and payload records) exchanged over Kafka.
 *
 * <p>Data shapes only: records, enums and sealed interfaces. No Spring and no business logic.
 * Commands are owned by the receiving service, events by the publishing service (ADR-0005).
 */
package io.github.pasindu9999.orderflow.contracts;
