/**
 * Messaging infrastructure shared by all services: envelope codec, transactional outbox
 * (writer and relay), idempotent inbox handler and Kafka error handling.
 *
 * <p>Infrastructure only. This module must never contain domain concepts or import from a service.
 */
package io.github.pasindu9999.orderflow.messaging;
