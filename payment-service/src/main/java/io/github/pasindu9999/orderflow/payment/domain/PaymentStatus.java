package io.github.pasindu9999.orderflow.payment.domain;

/** SUCCEEDED can become REFUNDED; FAILED and REFUNDED are final. A payment row is never deleted. */
public enum PaymentStatus {
    SUCCEEDED,
    FAILED,
    REFUNDED
}
