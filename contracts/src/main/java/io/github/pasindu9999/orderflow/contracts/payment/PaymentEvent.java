package io.github.pasindu9999.orderflow.contracts.payment;

import io.github.pasindu9999.orderflow.contracts.Message;

/** Events published by payment-service on {@code payment.events}. Owned by payment-service. */
public sealed interface PaymentEvent extends Message permits PaymentSucceeded, PaymentFailed, PaymentRefunded {
}
