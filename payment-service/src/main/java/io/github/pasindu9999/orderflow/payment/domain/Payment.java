package io.github.pasindu9999.orderflow.payment.domain;

import java.math.BigDecimal;
import java.util.UUID;

/** @param failureReason set only when {@code status} is FAILED */
public record Payment(UUID id, UUID orderId, UUID customerId, BigDecimal amount, String currency,
                      PaymentStatus status, DeclineReason failureReason) {
}
