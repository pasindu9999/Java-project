package io.github.pasindu9999.orderflow.order.api;

import java.util.UUID;

/** Body of the {@code 202 Accepted} answer to {@code POST /orders}. */
public record PlaceOrderResponse(UUID orderId, String status) {
}
