package io.github.pasindu9999.orderflow.payment.app;

/** The (simulated) provider is briefly unavailable. Retryable (ADR-0004): nothing was charged or stored. */
public class TransientPaymentException extends RuntimeException {

    public TransientPaymentException(String message) {
        super(message);
    }
}
