package io.github.pasindu9999.orderflow.order.api;

import io.github.pasindu9999.orderflow.order.domain.Order;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Body of {@code GET /orders/{id}}. Money is a decimal string, matching the Kafka wire format. */
public record OrderResponse(
        UUID orderId,
        UUID customerId,
        String status,
        String cancelReason,
        String totalAmount,
        String currency,
        List<Line> lines,
        Instant createdAt,
        Instant updatedAt) {

    public record Line(int lineNo, String sku, int quantity, String unitPrice) {
    }

    static OrderResponse from(Order order) {
        return new OrderResponse(
                order.id(),
                order.customerId(),
                order.status().name(),
                order.cancelReason() == null ? null : order.cancelReason().name(),
                order.totalAmount().toPlainString(),
                order.currency(),
                order.lines().stream()
                        .map(l -> new Line(l.lineNo(), l.sku(), l.quantity(), l.unitPrice().toPlainString()))
                        .toList(),
                order.createdAt(),
                order.updatedAt());
    }
}
