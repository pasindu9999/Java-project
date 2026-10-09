package io.github.pasindu9999.orderflow.order.api;

import io.github.pasindu9999.orderflow.order.app.OrderService;
import io.github.pasindu9999.orderflow.order.app.PlaceOrderResult;
import io.github.pasindu9999.orderflow.order.domain.Order;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@RestController
@RequestMapping("/orders")
class OrderController {

    static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    /** Tells a client its retry matched an existing order (same convention as Stripe's API). */
    static final String IDEMPOTENT_REPLAYED = "Idempotent-Replayed";

    private final OrderService orderService;

    OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * 202, not 201: the order exists, but the saga deciding its fate runs asynchronously. Clients poll the
     * Location until the status is CONFIRMED or CANCELLED.
     */
    @PostMapping
    ResponseEntity<PlaceOrderResponse> placeOrder(@RequestHeader(IDEMPOTENCY_KEY) String idempotencyKey,
                                                  @RequestBody CreateOrderRequest request) {
        PlaceOrderResult result = orderService.placeOrder(idempotencyKey, request.toDraft());
        Order order = result.order();
        URI location = ServletUriComponentsBuilder.fromCurrentRequest().path("/{id}").buildAndExpand(order.id()).toUri();
        return ResponseEntity.accepted()
                .location(location)
                .header(IDEMPOTENT_REPLAYED, String.valueOf(result.replayed()))
                .body(new PlaceOrderResponse(order.id(), order.status().name()));
    }

    @GetMapping("/{orderId}")
    OrderResponse getOrder(@PathVariable UUID orderId) {
        return OrderResponse.from(orderService.getOrder(orderId));
    }
}
