package io.github.pasindu9999.orderflow.order.app;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.pasindu9999.orderflow.contracts.Message;
import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryEvent;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryReserved;
import io.github.pasindu9999.orderflow.contracts.inventory.ReleaseInventory;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentFailed;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentSucceeded;
import io.github.pasindu9999.orderflow.contracts.payment.RefundPayment;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.github.pasindu9999.orderflow.messaging.testing.ProgrammableFaultInjector;
import io.github.pasindu9999.orderflow.order.TestcontainersConfiguration;
import io.github.pasindu9999.orderflow.order.domain.CancelReason;
import io.github.pasindu9999.orderflow.order.domain.Order;
import io.github.pasindu9999.orderflow.order.domain.OrderDraft;
import io.github.pasindu9999.orderflow.order.domain.OrderStatus;
import io.github.pasindu9999.orderflow.order.persistence.OrderRepository;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * The saga timeout (ARCHITECTURE §7.4, §8.7). The background schedule is off in tests: each test moves its own
 * order's deadline into the past, then calls {@link OrderTimeoutSweeper#sweep()} itself. Replies are faked by
 * publishing them to {@code inventory.events} / {@code payment.events}.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderTimeoutSweeperIT {

    @Autowired OrderTimeoutSweeper sweeper;
    @Autowired OrderService orderService;
    @Autowired OrderRepository orders;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired MessageCodec codec;
    @Autowired JdbcClient jdbc;
    @Autowired ProgrammableFaultInjector faults;

    UUID orderId;

    @BeforeEach
    void placeOrder() {
        OrderDraft draft = OrderDraft.of(UUID.randomUUID(), "EUR", List.of(
                new OrderDraft.LineInput("MUG-RED", 2, new BigDecimal("12.50"))));
        orderId = orderService.placeOrder(UUID.randomUUID().toString(), draft).order().id();
    }

    @AfterEach
    void disarmFaults() {
        faults.reset();
    }

    @Test
    void shouldCancelAndReleaseStock_whenInventoryNeverReplies() {
        expireDeadline();

        sweeper.sweep();

        assertCancelledByTimeout();
        assertThat(outboxTypes()).containsExactly("ReserveInventory", "ReleaseInventory");
        assertThat(lastOutboxMessage().payload())
                .as("inventory writes a tombstone, so a late reservation is refused")
                .isEqualTo(new ReleaseInventory(orderId, ReleaseInventory.Reason.TIMEOUT));
    }

    @Test
    void shouldCancelAndReleaseStock_whenPaymentNeverReplies() {
        awaitPayment();
        expireDeadline();

        sweeper.sweep();

        assertCancelledByTimeout();
        assertThat(outboxTypes()).containsExactly("ReserveInventory", "ProcessPayment", "ReleaseInventory");
        assertThat(lastOutboxMessage().payload()).isEqualTo(new ReleaseInventory(orderId, ReleaseInventory.Reason.TIMEOUT));
    }

    @Test
    void shouldLeaveOrderOpen_whenDeadlineHasNotPassed() {
        sweeper.sweep();

        assertThat(order().status()).isEqualTo(OrderStatus.PENDING);
        assertThat(outboxTypes()).containsExactly("ReserveInventory");
    }

    @Test
    void shouldRefund_whenPaymentSucceedsAfterTheOrderTimedOut() {
        awaitPayment();
        expireDeadline();
        sweeper.sweep();
        assertCancelledByTimeout();

        // The race §8.7 describes: charged just before the deadline, reply consumed after the timeout.
        Envelope late = reply(new PaymentSucceeded(orderId, UUID.randomUUID(), new BigDecimal("25.00"), "EUR"));
        await().atMost(15, SECONDS).until(() -> processed(late));

        assertCancelledByTimeout();
        assertThat(outboxTypes()).containsExactly("ReserveInventory", "ProcessPayment", "ReleaseInventory", "RefundPayment");
        Envelope refund = lastOutboxMessage();
        assertThat(refund.payload()).isEqualTo(new RefundPayment(orderId, RefundPayment.Reason.LATE_PAYMENT));
        assertThat(refund.causationId()).isEqualTo(late.messageId());
    }

    @Test
    void shouldOnlyRecordExpiredPayment_whenOrderAlreadyTimedOut() {
        awaitPayment();
        expireDeadline();
        sweeper.sweep();

        // The usual case §8.7 describes: payment consumes the stale command after the deadline and declines it.
        Envelope expired = reply(new PaymentFailed(orderId, PaymentFailed.Reason.EXPIRED));
        await().atMost(15, SECONDS).until(() -> processed(expired));

        assertCancelledByTimeout();
        assertThat(outboxTypes()).as("already released by the timeout")
                .containsExactly("ReserveInventory", "ProcessPayment", "ReleaseInventory");
    }

    @Test
    void shouldLeaveOrderToNextSweep_whenReplyWinsTheRaceAgainstTheSweeper() {
        expireDeadline();
        // The reply commits between the sweeper's read and its update. The sweeper waits for it on its own
        // thread, while the listener thread handles the reply: no row lock is held, so nothing blocks.
        Envelope reserved = codec.wrap(new InventoryReserved(orderId), null);
        faults.runOnce(OrderTimeoutSweeper.BEFORE_CANCEL, () -> {
            send(reserved);
            await().atMost(15, SECONDS).until(() -> processed(reserved));
        });

        sweeper.sweep();

        assertThat(faults.isArmed(OrderTimeoutSweeper.BEFORE_CANCEL)).as("the reply really slipped in").isFalse();
        assertThat(order().status()).as("the sweeper lost the version check").isEqualTo(OrderStatus.AWAITING_PAYMENT);
        assertThat(outboxTypes()).containsExactly("ReserveInventory", "ProcessPayment");

        sweeper.sweep(); // the deadline has still passed, so the next run cancels it

        assertCancelledByTimeout();
        assertThat(outboxTypes()).containsExactly("ReserveInventory", "ProcessPayment", "ReleaseInventory");
    }

    @Test
    void shouldReleaseLateReservation_whenSweeperWinsTheRaceAgainstTheReply() {
        expireDeadline();
        // The real sweeper runs between the saga's read and its update, on another thread, so it commits first.
        faults.runOnce(OrderSaga.BEFORE_STATUS_UPDATE, () -> CompletableFuture.supplyAsync(sweeper::sweep).join());

        Envelope reserved = reply(new InventoryReserved(orderId));

        // First attempt loses the version check and rolls back; the retried delivery reads CANCELLED.
        await().atMost(15, SECONDS).until(() -> processed(reserved));
        assertThat(faults.isArmed(OrderSaga.BEFORE_STATUS_UPDATE)).as("the sweeper really slipped in").isFalse();
        assertCancelledByTimeout();
        assertThat(outboxTypes()).as("never charged; releasing twice is harmless, release is idempotent")
                .containsExactly("ReserveInventory", "ReleaseInventory", "ReleaseInventory");
        assertThat(lastOutboxMessage().payload())
                .isEqualTo(new ReleaseInventory(orderId, ReleaseInventory.Reason.LATE_RESERVATION));
    }

    /**
     * Far in the past, so this order sorts ahead of any order other test classes left behind in the shared
     * database (the sweeper takes the oldest deadlines first, at most one batch per run).
     */
    private void expireDeadline() {
        jdbc.sql("UPDATE orders SET deadline_at = now() - interval '1 day' WHERE id = :id").param("id", orderId).update();
    }

    private void awaitPayment() {
        Envelope reserved = reply(new InventoryReserved(orderId));
        await().atMost(15, SECONDS).until(() -> processed(reserved));
        assertThat(order().status()).isEqualTo(OrderStatus.AWAITING_PAYMENT);
    }

    private void assertCancelledByTimeout() {
        Order order = order();
        assertThat(order.status()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.cancelReason()).isEqualTo(CancelReason.TIMEOUT);
    }

    private Envelope reply(Message event) {
        Envelope envelope = codec.wrap(event, null);
        send(envelope);
        return envelope;
    }

    /** Publishes a fake reply on the topic its real producer would use. */
    private void send(Envelope envelope) {
        String topic = envelope.payload() instanceof InventoryEvent ? Topics.INVENTORY_EVENTS : Topics.PAYMENT_EVENTS;
        kafka.send(topic, orderId.toString(), codec.encode(envelope)).join();
    }

    private Order order() {
        return orders.findById(orderId).orElseThrow();
    }

    private boolean processed(Envelope envelope) {
        return jdbc.sql("SELECT count(*) = 1 FROM processed_message WHERE message_id = :id")
                .param("id", envelope.messageId())
                .query(Boolean.class)
                .single();
    }

    private List<String> outboxTypes() {
        return jdbc.sql("SELECT message_type FROM outbox WHERE message_key = :key ORDER BY id")
                .param("key", orderId.toString())
                .query(String.class)
                .list();
    }

    private Envelope lastOutboxMessage() {
        String envelope = jdbc.sql("SELECT envelope::text FROM outbox WHERE message_key = :key ORDER BY id DESC LIMIT 1")
                .param("key", orderId.toString())
                .query(String.class)
                .single();
        return codec.decode(envelope);
    }
}
