package io.github.pasindu9999.orderflow.order.app;

/** The client sent an idempotency key it already used for a different request. */
public class IdempotencyKeyReusedException extends RuntimeException {

    public IdempotencyKeyReusedException(String idempotencyKey) {
        super("Idempotency-Key '%s' was already used for a different request".formatted(idempotencyKey));
    }
}
