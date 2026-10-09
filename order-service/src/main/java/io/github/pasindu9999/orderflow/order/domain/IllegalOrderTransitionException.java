package io.github.pasindu9999.orderflow.order.domain;

import java.util.UUID;

/** A status change the state machine doesn't allow, e.g. CANCELLED to CONFIRMED. Always a programming error. */
public class IllegalOrderTransitionException extends RuntimeException {

    public IllegalOrderTransitionException(UUID orderId, OrderStatus from, OrderStatus to) {
        super("Order %s cannot move from %s to %s".formatted(orderId, from, to));
    }
}
