package io.github.pasindu9999.orderflow.order.domain;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * An order and its saga state. Immutable: every transition returns a new instance, and the repository persists
 * it with an optimistic {@code version} check.
 */
public record Order(
        UUID id,
        UUID customerId,
        OrderStatus status,
        CancelReason cancelReason,
        BigDecimal totalAmount,
        String currency,
        String idempotencyKey,
        String requestHash,
        Instant deadlineAt,
        Instant createdAt,
        Instant updatedAt,
        long version,
        List<OrderLine> lines) {

    public static final int MAX_IDEMPOTENCY_KEY_LENGTH = 100;

    public Order {
        lines = List.copyOf(lines);
    }

    /** A new order starts PENDING, and its saga must finish before {@code now + sagaTimeout}. */
    public static Order place(UUID id, OrderDraft draft, String idempotencyKey, Instant now, Duration sagaTimeout) {
        requireValidIdempotencyKey(idempotencyKey);
        return new Order(id, draft.customerId(), OrderStatus.PENDING, null, draft.total(), draft.currency(),
                idempotencyKey, draft.fingerprint(), now.plus(sagaTimeout), now, now, 0, draft.lines());
    }

    public static void requireValidIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            throw new InvalidOrderException(List.of(
                    "Idempotency-Key must be 1 to " + MAX_IDEMPOTENCY_KEY_LENGTH + " characters"));
        }
    }

    public Order awaitPayment(Instant now) {
        return transitionTo(OrderStatus.AWAITING_PAYMENT, null, now);
    }

    public Order confirm(Instant now) {
        return transitionTo(OrderStatus.CONFIRMED, null, now);
    }

    public Order cancel(CancelReason reason, Instant now) {
        return transitionTo(OrderStatus.CANCELLED, Objects.requireNonNull(reason, "reason"), now);
    }

    private Order transitionTo(OrderStatus next, CancelReason reason, Instant now) {
        if (!status.canTransitionTo(next)) {
            throw new IllegalOrderTransitionException(id, status, next);
        }
        return new Order(id, customerId, next, reason, totalAmount, currency, idempotencyKey, requestHash,
                deadlineAt, createdAt, now, version, lines);
    }
}
