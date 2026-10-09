package io.github.pasindu9999.orderflow.contracts.payment;

import java.util.UUID;

/** Compensation for a charge that arrived after the order was cancelled. */
public record RefundPayment(UUID orderId, Reason reason) implements PaymentCommand {

    public enum Reason {
        LATE_PAYMENT
    }
}
