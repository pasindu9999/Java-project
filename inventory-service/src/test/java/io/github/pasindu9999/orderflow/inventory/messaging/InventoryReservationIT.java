package io.github.pasindu9999.orderflow.inventory.messaging;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.pasindu9999.orderflow.contracts.Message;
import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryRejected;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryReserved;
import io.github.pasindu9999.orderflow.contracts.inventory.ReserveInventory;
import io.github.pasindu9999.orderflow.inventory.TestcontainersConfiguration;
import io.github.pasindu9999.orderflow.inventory.domain.ReservationLine;
import io.github.pasindu9999.orderflow.inventory.domain.ReservationStatus;
import io.github.pasindu9999.orderflow.inventory.domain.StockLevel;
import io.github.pasindu9999.orderflow.inventory.persistence.ReservationRepository;
import io.github.pasindu9999.orderflow.inventory.persistence.StockRepository;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.github.pasindu9999.orderflow.messaging.testing.KafkaTopicReader;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * The test plays order-service: it publishes commands to {@code inventory.commands} and reads the replies on
 * {@code inventory.events}. Every test uses its own order and its own freshly created SKUs.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class InventoryReservationIT {

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired MessageCodec codec;
    @Autowired JdbcClient jdbc;
    @Autowired StockRepository stock;
    @Autowired ReservationRepository reservations;
    @Autowired MeterRegistry meters;

    final UUID orderId = UUID.randomUUID();
    final String mug = newSku("MUG");
    final String tea = newSku("TEA");

    @Test
    void shouldReserveEveryLineAndReply_whenStockIsSufficient() {
        createStock(mug, 10);
        createStock(tea, 5);

        Envelope command = send(new ReserveInventory(orderId, List.of(
                new ReserveInventory.Line(mug, 2), new ReserveInventory.Line(tea, 1), new ReserveInventory.Line(mug, 1))));

        Envelope reply = awaitSingleReply();
        assertThat(reply.payload()).isEqualTo(new InventoryReserved(orderId));
        assertThat(reply.causationId()).as("reply points at the command that caused it").isEqualTo(command.messageId());
        assertThat(stock.find(mug)).contains(new StockLevel(mug, 7, 3));
        assertThat(stock.find(tea)).contains(new StockLevel(tea, 4, 1));
        assertThat(reservations.findStatus(orderId)).contains(ReservationStatus.RESERVED);
        assertThat(reservations.findLines(orderId)).containsExactlyInAnyOrder(
                new ReservationLine(mug, 3), new ReservationLine(tea, 1));
    }

    @Test
    void shouldRejectWithoutTouchingStock_whenOneLineIsShort() {
        createStock(mug, 10);
        createStock(tea, 1);

        send(new ReserveInventory(orderId, List.of(new ReserveInventory.Line(mug, 2), new ReserveInventory.Line(tea, 2))));

        assertThat(awaitSingleReply().payload()).isEqualTo(new InventoryRejected(orderId,
                InventoryRejected.Reason.OUT_OF_STOCK, List.of(new InventoryRejected.Shortage(tea, 2, 1))));
        assertThat(stock.find(mug)).as("all or nothing: the satisfiable line wasn't reserved either")
                .contains(new StockLevel(mug, 10, 0));
        assertThat(stock.find(tea)).contains(new StockLevel(tea, 1, 0));
        assertThat(reservations.findStatus(orderId)).contains(ReservationStatus.REJECTED);
    }

    @Test
    void shouldRejectAsUnknownSku_whenSkuDoesNotExist() {
        String ghost = newSku("GHOST");

        send(new ReserveInventory(orderId, List.of(new ReserveInventory.Line(ghost, 1))));

        assertThat(awaitSingleReply().payload()).isEqualTo(new InventoryRejected(orderId,
                InventoryRejected.Reason.UNKNOWN_SKU, List.of(new InventoryRejected.Shortage(ghost, 1, 0))));
    }

    @Test
    void shouldReserveOnceAndReplyOnce_whenSameCommandIsDeliveredTwice() {
        createStock(mug, 10);
        double duplicatesBefore = duplicatesSkipped();
        Envelope command = codec.wrap(new ReserveInventory(orderId, List.of(new ReserveInventory.Line(mug, 4))), null);
        String record = codec.encode(command);

        sendRaw(record);
        sendRaw(record); // e.g. order-service's relay crashed after sending, before marking the row

        await().atMost(15, SECONDS).until(() -> duplicatesSkipped() >= duplicatesBefore + 1);
        assertThat(awaitSingleReply().payload()).isEqualTo(new InventoryReserved(orderId));
        assertThat(stock.find(mug)).contains(new StockLevel(mug, 6, 4));
    }

    @Test
    void shouldKeepFirstOutcome_whenADifferentCommandArrivesForTheSameOrder() {
        createStock(mug, 10);
        send(new ReserveInventory(orderId, List.of(new ReserveInventory.Line(mug, 4))));
        awaitSingleReply();

        // New messageId, so the inbox can't catch it; the reservation row (natural key) does.
        Envelope second = send(new ReserveInventory(orderId, List.of(new ReserveInventory.Line(mug, 5))));
        await().atMost(15, SECONDS).until(() -> processed(second));

        assertThat(replies()).hasSize(1);
        assertThat(stock.find(mug)).contains(new StockLevel(mug, 6, 4));
    }

    private Envelope send(Message command) {
        Envelope envelope = codec.wrap(command, null);
        sendRaw(codec.encode(envelope));
        return envelope;
    }

    private void sendRaw(String record) {
        kafka.send(Topics.INVENTORY_COMMANDS, orderId.toString(), record).join();
    }

    private Envelope awaitSingleReply() {
        List<String> replies = await().atMost(15, SECONDS).until(this::replies, r -> !r.isEmpty());
        assertThat(replies).hasSize(1);
        return codec.decode(replies.getFirst());
    }

    private List<String> replies() {
        return KafkaTopicReader.readKey(kafkaAdmin.getConfigurationProperties(), Topics.INVENTORY_EVENTS, orderId.toString())
                .stream().map(r -> r.value()).toList();
    }

    private boolean processed(Envelope envelope) {
        return jdbc.sql("SELECT count(*) = 1 FROM processed_message WHERE consumer = :consumer AND message_id = :id")
                .param("consumer", InventoryCommandListener.CONSUMER)
                .param("id", envelope.messageId())
                .query(Boolean.class)
                .single();
    }

    private double duplicatesSkipped() {
        var counter = meters.find("messaging.duplicates.skipped").tag("consumer", InventoryCommandListener.CONSUMER).counter();
        return counter == null ? 0 : counter.count();
    }

    private void createStock(String sku, int available) {
        jdbc.sql("INSERT INTO product_stock (sku, available, reserved) VALUES (:sku, :available, 0)")
                .param("sku", sku)
                .param("available", available)
                .update();
    }

    private static String newSku(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
