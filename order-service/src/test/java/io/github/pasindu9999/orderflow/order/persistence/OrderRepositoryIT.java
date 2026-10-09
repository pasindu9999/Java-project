package io.github.pasindu9999.orderflow.order.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.pasindu9999.orderflow.order.TestcontainersConfiguration;
import io.github.pasindu9999.orderflow.order.domain.CancelReason;
import io.github.pasindu9999.orderflow.order.domain.Order;
import io.github.pasindu9999.orderflow.order.domain.OrderDraft;
import io.github.pasindu9999.orderflow.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.OptimisticLockingFailureException;

/** Status changes are optimistic updates on {@code version} (ARCHITECTURE §3.3). */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderRepositoryIT {

    @Autowired OrderRepository orders;

    final Instant now = Instant.now().truncatedTo(ChronoUnit.MICROS);

    @Test
    void shouldSaveStatusAndBumpVersion_whenRowIsUnchangedSinceRead() {
        Order placed = insertOrder();

        orders.updateStatus(placed.awaitPayment(now));

        Order saved = orders.findById(placed.id()).orElseThrow();
        assertThat(saved.status()).isEqualTo(OrderStatus.AWAITING_PAYMENT);
        assertThat(saved.version()).isEqualTo(1);
    }

    @Test
    void shouldRejectStaleWrite_whenRowChangedSinceRead() {
        Order placed = insertOrder();
        Order readByA = orders.findById(placed.id()).orElseThrow();
        Order readByB = orders.findById(placed.id()).orElseThrow();
        orders.updateStatus(readByA.cancel(CancelReason.TIMEOUT, now));

        assertThatThrownBy(() -> orders.updateStatus(readByB.awaitPayment(now)))
                .isInstanceOf(OptimisticLockingFailureException.class);

        Order saved = orders.findById(placed.id()).orElseThrow();
        assertThat(saved.status()).as("the first writer's change survives").isEqualTo(OrderStatus.CANCELLED);
        assertThat(saved.cancelReason()).isEqualTo(CancelReason.TIMEOUT);
        assertThat(saved.version()).isEqualTo(1);
    }

    private Order insertOrder() {
        OrderDraft draft = OrderDraft.of(UUID.randomUUID(), "EUR",
                List.of(new OrderDraft.LineInput("MUG-RED", 1, new BigDecimal("12.50"))));
        Order order = Order.place(UUID.randomUUID(), draft, UUID.randomUUID().toString(), now, Duration.ofSeconds(30));
        orders.insert(order);
        return order;
    }
}
