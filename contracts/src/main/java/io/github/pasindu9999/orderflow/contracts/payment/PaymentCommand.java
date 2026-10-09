package io.github.pasindu9999.orderflow.contracts.payment;

import io.github.pasindu9999.orderflow.contracts.Message;

/** Commands handled by payment-service, published on {@code payment.commands}. Owned by payment-service. */
public sealed interface PaymentCommand extends Message permits ProcessPayment, RefundPayment {
}
