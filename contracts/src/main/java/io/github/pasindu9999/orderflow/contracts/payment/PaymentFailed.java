package io.github.pasindu9999.orderflow.contracts.payment;

import java.util.UUID;

public record PaymentFailed(UUID orderId, Reason reason) implements PaymentEvent {

    public enum Reason {
        DECLINED_LIMIT,
        DECLINED_BLOCKED_CUSTOMER,
        EXPIRED
    }
}
