package io.github.pasindu9999.orderflow.contracts.payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Charge the customer. {@code expiresAt} is the order's saga deadline: payment-service declines the command
 * once it has passed, so a slow delivery can't charge an order that has already been cancelled.
 */
public record ProcessPayment(UUID orderId, UUID customerId, BigDecimal amount, String currency, Instant expiresAt)
        implements PaymentCommand {
}
