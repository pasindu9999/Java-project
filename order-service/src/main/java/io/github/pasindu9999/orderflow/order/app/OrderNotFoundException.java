package io.github.pasindu9999.orderflow.order.app;

import java.util.UUID;

public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(UUID orderId) {
        super("Order " + orderId + " does not exist");
    }
}
