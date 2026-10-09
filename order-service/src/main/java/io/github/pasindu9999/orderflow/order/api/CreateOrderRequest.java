package io.github.pasindu9999.orderflow.order.api;

import io.github.pasindu9999.orderflow.order.domain.OrderDraft;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/** Body of {@code POST /orders}. Validation happens in {@link OrderDraft#of}, not here. */
public record CreateOrderRequest(UUID customerId, String currency, List<Line> lines) {

    public record Line(String sku, Integer quantity, BigDecimal unitPrice) {
    }

    OrderDraft toDraft() {
        List<OrderDraft.LineInput> inputs = lines == null ? null : lines.stream()
                .map(l -> l == null ? null : new OrderDraft.LineInput(l.sku(), l.quantity(), l.unitPrice()))
                .toList();
        return OrderDraft.of(customerId, currency, inputs);
    }
}
