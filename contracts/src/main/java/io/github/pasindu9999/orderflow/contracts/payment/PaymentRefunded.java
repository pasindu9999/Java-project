package io.github.pasindu9999.orderflow.contracts.payment;

import java.util.UUID;

public record PaymentRefunded(UUID orderId, UUID paymentId) implements PaymentEvent {
}
