package io.github.pasindu9999.orderflow.contracts.payment;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentSucceeded(UUID orderId, UUID paymentId, BigDecimal amount, String currency)
        implements PaymentEvent {
}
