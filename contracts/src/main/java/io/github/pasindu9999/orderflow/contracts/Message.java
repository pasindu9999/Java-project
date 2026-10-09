package io.github.pasindu9999.orderflow.contracts;

import java.util.UUID;

/**
 * Payload of any message exchanged between services. Every message belongs to exactly one order, which is
 * also its Kafka key and correlation id.
 */
public interface Message {

    UUID orderId();
}
