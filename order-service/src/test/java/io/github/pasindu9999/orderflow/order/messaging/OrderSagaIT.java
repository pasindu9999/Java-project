package io.github.pasindu9999.orderflow.order.messaging;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.pasindu9999.orderflow.contracts.Message;
import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryRejected;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryEvent;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryReserved;
import io.github.pasindu9999.orderflow.contracts.inventory.ReleaseInventory;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentFailed;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentSucceeded;
import io.github.pasindu9999.orderflow.contracts.payment.ProcessPayment;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.github.pasindu9999.orderflow.messaging.error.DeadLetterErrorHandler;
import io.github.pasindu9999.orderflow.messaging.testing.KafkaTopicReader;
import io.github.pasindu9999.orderflow.order.TestcontainersConfiguration;
import io.github.pasindu9999.orderflow.order.app.OrderService;
import io.github.pasindu9999.orderflow.order.domain.CancelReason;
import io.github.pasindu9999.orderflow.order.domain.Order;
import io.github.pasindu9999.orderflow.order.domain.OrderDraft;
import io.github.pasindu9999.orderflow.order.domain.OrderStatus;
import io.github.pasindu9999.orderflow.order.persistence.OrderRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * The reply handling table (ARCHITECTURE §3.3). The test plays inventory-service and payment-service: it publishes
 * replies to {@code inventory.events} and {@code payment.events} and checks the order's state and the commands it
 * emitted.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OrderSagaIT {

    @Autowired OrderService orderService;
    @Autowired OrderRepository orders;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired MessageCodec codec;
    @Autowired JdbcClient jdbc;
    @Autowired MeterRegistry meters;

    UUID orderId;

    @BeforeEach
    void placeOrder() {
        OrderDraft draft = OrderDraft.of(UUID.randomUUID(), "EUR", List.of(
                new OrderDraft.LineInput("MUG-RED", 2, new BigDecimal("12.50")),
                new OrderDraft.LineInput("TEA-GREEN", 1, new BigDecimal("8.00"))));
        orderId = orderService.placeOrder(UUID.randomUUID().toString(), draft).order().id();
    }

    @Test
    void shouldAwaitPaymentAndSendProcessPayment_whenInventoryIsReserved() {
        Envelope reply = reply(new InventoryReserved(orderId));

        await().atMost(15, SECONDS).until(() -> order().status() == OrderStatus.AWAITING_PAYMENT);
        Order order = order();
        assertThat(order.version()).isEqualTo(1);

        List<String> records = await().atMost(15, SECONDS)
                .until(() -> read(Topics.PAYMENT_COMMANDS), r -> !r.isEmpty());
        assertThat(records).hasSize(1);
        Envelope command = codec.decode(records.getFirst());
        assertThat(command.payload()).isEqualTo(new ProcessPayment(
                orderId, order.customerId(), new BigDecimal("33.00"), "EUR", order.deadlineAt()));
        assertThat(command.causationId()).isEqualTo(reply.messageId());
    }

    @Test
    void shouldCancelAsOutOfStockWithoutCharging_whenInventoryIsRejected() {
        Envelope reply = reply(new InventoryRejected(orderId, InventoryRejected.Reason.OUT_OF_STOCK,
                List.of(new InventoryRejected.Shortage("MUG-RED", 2, 1))));

        await().atMost(15, SECONDS).until(() -> processed(reply));
        assertThat(order().status()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order().cancelReason()).isEqualTo(CancelReason.OUT_OF_STOCK);
        assertThat(outboxTypes()).containsExactly("ReserveInventory");
    }

    @Test
    void shouldSendOneProcessPayment_whenSameReplyIsDeliveredTwice() {
        double duplicatesBefore = duplicatesSkipped();
        String record = codec.encode(codec.wrap(new InventoryReserved(orderId), null));

        sendRaw(record);
        sendRaw(record); // e.g. inventory's relay crashed after sending, before marking its outbox row

        await().atMost(15, SECONDS).until(() -> duplicatesSkipped() >= duplicatesBefore + 1);
        assertThat(order().status()).isEqualTo(OrderStatus.AWAITING_PAYMENT);
        assertThat(outboxTypes()).containsExactly("ReserveInventory", "ProcessPayment");
    }

    @Test
    void shouldIgnoreSecondReservedReply_whenOrderAlreadyAwaitsPayment() {
        Envelope first = reply(new InventoryReserved(orderId));
        await().atMost(15, SECONDS).until(() -> processed(first));

        // New messageId, so the inbox can't catch it; the state machine does.
        Envelope second = reply(new InventoryReserved(orderId));
        await().atMost(15, SECONDS).until(() -> processed(second));

        assertThat(order().status()).isEqualTo(OrderStatus.AWAITING_PAYMENT);
        assertThat(order().version()).as("ignored replies don't touch the row").isEqualTo(1);
        assertThat(outboxTypes()).containsExactly("ReserveInventory", "ProcessPayment");
    }

    @Test
    void shouldOnlyRecordLateRejection_whenOrderIsAlreadyCancelled() {
        cancelOutOfStock();
        long versionBefore = order().version();

        Envelope late = reply(new InventoryRejected(orderId, InventoryRejected.Reason.UNKNOWN_SKU, List.of()));
        await().atMost(15, SECONDS).until(() -> processed(late));

        assertThat(order().version()).isEqualTo(versionBefore);
        assertThat(outboxTypes()).containsExactly("ReserveInventory");
    }

    @Test
    void shouldConfirm_whenPaymentSucceeds() {
        awaitPayment();

        Envelope reply = reply(new PaymentSucceeded(orderId, UUID.randomUUID(), new BigDecimal("33.00"), "EUR"));
        await().atMost(15, SECONDS).until(() -> processed(reply));

        assertThat(order().status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(order().version()).isEqualTo(2);
        assertThat(outboxTypes()).as("confirming needs no command").containsExactly("ReserveInventory", "ProcessPayment");
    }

    @Test
    void shouldCancelAndReleaseStock_whenPaymentFails() {
        awaitPayment();

        Envelope reply = reply(new PaymentFailed(orderId, PaymentFailed.Reason.DECLINED_LIMIT));
        await().atMost(15, SECONDS).until(() -> processed(reply));

        assertThat(order().status()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order().cancelReason()).isEqualTo(CancelReason.PAYMENT_DECLINED);
        assertThat(outboxTypes()).containsExactly("ReserveInventory", "ProcessPayment", "ReleaseInventory");
        Envelope release = lastOutboxMessage();
        assertThat(release.payload()).isEqualTo(new ReleaseInventory(orderId, ReleaseInventory.Reason.PAYMENT_DECLINED));
        assertThat(release.causationId()).isEqualTo(reply.messageId());
    }

    @Test
    void shouldIgnorePaymentReply_whenOrderIsStillPending() {
        Envelope reply = reply(new PaymentSucceeded(orderId, UUID.randomUUID(), new BigDecimal("33.00"), "EUR"));
        await().atMost(15, SECONDS).until(() -> processed(reply));

        assertThat(order().status()).isEqualTo(OrderStatus.PENDING);
        assertThat(order().version()).isZero();
    }

    @Test
    void shouldOnlyRecordLatePaymentFailure_whenOrderIsAlreadyCancelled() {
        cancelOutOfStock();
        long versionBefore = order().version();

        Envelope late = reply(new PaymentFailed(orderId, PaymentFailed.Reason.EXPIRED));
        await().atMost(15, SECONDS).until(() -> processed(late));

        assertThat(order().version()).isEqualTo(versionBefore);
        assertThat(outboxTypes()).as("nothing to release: inventory never reserved").containsExactly("ReserveInventory");
    }

    @Test
    void shouldDeadLetterReply_whenOrderIsUnknown() {
        UUID unknown = UUID.randomUUID();
        kafka.send(Topics.INVENTORY_EVENTS, unknown.toString(), codec.encode(codec.wrap(new InventoryReserved(unknown), null))).join();

        List<String> parked = await().atMost(15, SECONDS).until(
                () -> KafkaTopicReader.readKey(kafkaAdmin.getConfigurationProperties(),
                                Topics.INVENTORY_EVENTS + DeadLetterErrorHandler.DLT_SUFFIX, unknown.toString())
                        .stream().map(r -> r.value()).toList(),
                r -> !r.isEmpty());
        assertThat(parked).hasSize(1);
    }

    private void awaitPayment() {
        Envelope reserved = reply(new InventoryReserved(orderId));
        await().atMost(15, SECONDS).until(() -> processed(reserved));
        assertThat(order().status()).isEqualTo(OrderStatus.AWAITING_PAYMENT);
    }

    private void cancelOutOfStock() {
        Envelope rejected = reply(new InventoryRejected(orderId, InventoryRejected.Reason.OUT_OF_STOCK, List.of()));
        await().atMost(15, SECONDS).until(() -> processed(rejected));
        assertThat(order().status()).isEqualTo(OrderStatus.CANCELLED);
    }

    /** Publishes a fake reply on the topic its real producer would use. */
    private Envelope reply(Message event) {
        Envelope envelope = codec.wrap(event, null);
        String topic = event instanceof InventoryEvent ? Topics.INVENTORY_EVENTS : Topics.PAYMENT_EVENTS;
        kafka.send(topic, orderId.toString(), codec.encode(envelope)).join();
        return envelope;
    }

    private void sendRaw(String record) {
        kafka.send(Topics.INVENTORY_EVENTS, orderId.toString(), record).join();
    }

    private Order order() {
        return orders.findById(orderId).orElseThrow();
    }

    /** Every reply has a fresh messageId, so the inbox row alone identifies it, whichever listener handled it. */
    private boolean processed(Envelope envelope) {
        return jdbc.sql("SELECT count(*) = 1 FROM processed_message WHERE message_id = :id")
                .param("id", envelope.messageId())
                .query(Boolean.class)
                .single();
    }

    /** Commands the order emitted, oldest first. Written in the same transaction as the inbox row, so exact. */
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

    private List<String> read(String topic) {
        return KafkaTopicReader.readKey(kafkaAdmin.getConfigurationProperties(), topic, orderId.toString())
                .stream().map(r -> r.value()).toList();
    }

    private double duplicatesSkipped() {
        var counter = meters.find("messaging.duplicates.skipped").tag("consumer", InventoryEventListener.CONSUMER).counter();
        return counter == null ? 0 : counter.count();
    }
}
