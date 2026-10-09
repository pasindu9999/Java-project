package io.github.pasindu9999.orderflow.order.app;

import io.github.pasindu9999.orderflow.order.domain.Order;

/** @param replayed true when the order already existed for this idempotency key */
public record PlaceOrderResult(Order order, boolean replayed) {
}
