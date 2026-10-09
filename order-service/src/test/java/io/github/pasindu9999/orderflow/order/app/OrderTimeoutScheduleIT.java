package io.github.pasindu9999.orderflow.order.app;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.pasindu9999.orderflow.order.TestcontainersConfiguration;
import io.github.pasindu9999.orderflow.order.domain.CancelReason;
import io.github.pasindu9999.orderflow.order.domain.Order;
import io.github.pasindu9999.orderflow.order.domain.OrderDraft;
import io.github.pasindu9999.orderflow.order.domain.OrderStatus;
import io.github.pasindu9999.orderflow.order.persistence.OrderRepository;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * The background schedule itself, which the other ITs switch off. Its own Spring context (different properties),
 * with a short saga timeout. No inventory-service runs here, so nothing ever replies: the order can only end
 * through the sweeper.
 */
@SpringBootTest(properties = {
        "orderflow.saga.sweeper-enabled=true",
        "orderflow.saga.timeout=PT2S",
        "orderflow.saga.sweep-interval=PT0.5S"})
@Import(TestcontainersConfiguration.class)
class OrderTimeoutScheduleIT {

    @Autowired OrderService orderService;
    @Autowired OrderRepository orders;

    @Test
    void shouldCancelOrderInTheBackground_whenNoReplyArrivesBeforeTheDeadline() {
        OrderDraft draft = OrderDraft.of(UUID.randomUUID(), "EUR", List.of(
                new OrderDraft.LineInput("MUG-RED", 1, new BigDecimal("12.50"))));
        UUID orderId = orderService.placeOrder(UUID.randomUUID().toString(), draft).order().id();

        Order cancelled = await().atMost(15, SECONDS)
                .until(() -> orders.findById(orderId).orElseThrow(), order -> order.status() == OrderStatus.CANCELLED);

        assertThat(cancelled.cancelReason()).isEqualTo(CancelReason.TIMEOUT);
    }
}
