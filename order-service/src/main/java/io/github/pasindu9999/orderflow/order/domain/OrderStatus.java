package io.github.pasindu9999.orderflow.order.domain;

/** The order's saga state (ARCHITECTURE §3.2). CONFIRMED and CANCELLED are terminal. */
public enum OrderStatus {
    PENDING,
    AWAITING_PAYMENT,
    CONFIRMED,
    CANCELLED;

    public boolean isTerminal() {
        return this == CONFIRMED || this == CANCELLED;
    }

    public boolean canTransitionTo(OrderStatus next) {
        return switch (this) {
            case PENDING -> next == AWAITING_PAYMENT || next == CANCELLED;
            case AWAITING_PAYMENT -> next == CONFIRMED || next == CANCELLED;
            case CONFIRMED, CANCELLED -> false;
        };
    }
}
