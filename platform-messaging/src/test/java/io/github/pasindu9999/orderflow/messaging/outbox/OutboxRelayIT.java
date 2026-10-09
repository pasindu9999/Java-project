package io.github.pasindu9999.orderflow.messaging.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.pasindu9999.orderflow.contracts.Message;
import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.contracts.inventory.ReleaseInventory;
import io.github.pasindu9999.orderflow.contracts.inventory.ReserveInventory;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.github.pasindu9999.orderflow.messaging.testing.KafkaTopicReader;
import io.github.pasindu9999.orderflow.messaging.testing.ProgrammableFaultInjector;
import io.github.pasindu9999.orderflow.messaging.testing.ProgrammableFaultInjector.InjectedFault;
import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(classes = MessagingTestApplication.class)
class OutboxRelayIT {

    @Autowired OutboxWriter writer;
    @Autowired OutboxRelay relay;
    @Autowired MessageCodec codec;
    @Autowired ProgrammableFaultInjector faults;
    @Autowired TransactionTemplate transaction;
    @Autowired JdbcClient jdbc;
    @Autowired DataSource dataSource;
    @Autowired KafkaAdmin kafkaAdmin;

    final UUID orderId = UUID.randomUUID();

    @BeforeEach
    void emptyOutbox() {
        // publishBatch() relays every pending row, so each test starts from an empty outbox.
        jdbc.sql("DELETE FROM outbox").update();
    }

    @AfterEach
    void disarmFaults() {
        faults.reset();
    }

    @Test
    void shouldPublishRowAndMarkIt_whenRelayRuns() {
        Envelope written = write(reserve());

        assertThat(relay.publishBatch()).isEqualTo(1);

        List<ConsumerRecord<String, String>> records = records();
        assertThat(records).hasSize(1);
        assertThat(records.getFirst().key()).isEqualTo(orderId.toString());
        assertThat(codec.decode(records.getFirst().value())).isEqualTo(written);
        assertThat(pendingRows()).isZero();
    }

    @Test
    void shouldPublishInWriteOrder_whenSeveralMessagesForOneOrderAreQueued() {
        List<UUID> written = List.of(
                write(reserve()).messageId(),
                write(new ReleaseInventory(orderId, ReleaseInventory.Reason.PAYMENT_DECLINED)).messageId(),
                write(reserve()).messageId());

        relay.publishBatch();

        assertThat(records()).extracting(r -> codec.decode(r.value()).messageId()).containsExactlyElementsOf(written);
    }

    @Test
    void shouldResendSameMessage_whenRelayCrashesAfterSendBeforeMarking() {
        Envelope written = write(reserve());
        faults.failOnce(OutboxRelay.AFTER_SEND);

        assertThatThrownBy(relay::publishBatch).isInstanceOf(InjectedFault.class);
        assertThat(pendingRows()).as("crash rolled back the mark").isEqualTo(1);

        assertThat(relay.publishBatch()).isEqualTo(1);

        // At-least-once: the message went out twice, with the same messageId, so consumers can drop the copy.
        assertThat(records()).extracting(r -> codec.decode(r.value()).messageId())
                .containsExactly(written.messageId(), written.messageId());
        assertThat(pendingRows()).isZero();
    }

    @Test
    void shouldStopAtFailedSendAndKeepEarlierMarks_whenKafkaRejectsOneMessage() {
        Envelope first = write(reserve());
        Envelope second = write(reserve());
        Envelope third = write(reserve());
        faults.failOnCall(OutboxRelay.BEFORE_SEND, 2);

        assertThat(relay.publishBatch()).as("only the row before the failure").isEqualTo(1);
        assertThat(records()).extracting(r -> codec.decode(r.value()).messageId()).containsExactly(first.messageId());
        assertThat(pendingRows()).as("the failed row and everything after it wait").isEqualTo(2);

        assertThat(relay.publishBatch()).isEqualTo(2);
        assertThat(records()).extracting(r -> codec.decode(r.value()).messageId())
                .containsExactly(first.messageId(), second.messageId(), third.messageId());
    }

    @Test
    void shouldPublishNothing_whenAnotherInstanceHoldsTheRelayLock() throws Exception {
        write(reserve());

        try (Connection otherInstance = dataSource.getConnection()) {
            otherInstance.createStatement().execute("SELECT pg_advisory_lock(" + OutboxRelay.RELAY_LOCK_ID + ")");

            assertThat(relay.publishBatch()).isZero();
            assertThat(pendingRows()).isEqualTo(1);
        } // closing the session releases its advisory lock

        assertThat(relay.publishBatch()).isEqualTo(1);
    }

    @Test
    void shouldRollBackMessage_whenBusinessTransactionFails() {
        assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
            writer.write(reserve(), null);
            throw new IllegalStateException("business rule failed after the message was written");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(pendingRows()).isZero();
    }

    @Test
    void shouldRefuseToWrite_whenNoTransactionIsActive() {
        assertThatThrownBy(() -> writer.write(reserve(), null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("inside the business transaction");
    }

    @Test
    void shouldReportPendingCountAndAge_whenRowsAreWaiting() {
        OutboxMetrics metrics = new OutboxMetrics(jdbc);
        assertThat(metrics.pending()).isZero();
        assertThat(metrics.oldestAgeSeconds()).isZero();

        write(reserve());
        write(reserve());
        jdbc.sql("UPDATE outbox SET created_at = now() - interval '42 seconds'").update();

        assertThat(metrics.pending()).isEqualTo(2);
        assertThat(metrics.oldestAgeSeconds()).isBetween(42.0, 60.0);

        relay.publishBatch();
        assertThat(metrics.pending()).isZero();
        assertThat(metrics.oldestAgeSeconds()).isZero();
    }

    private ReserveInventory reserve() {
        return new ReserveInventory(orderId, List.of(new ReserveInventory.Line("MUG-RED", 1)));
    }

    private Envelope write(Message message) {
        return transaction.execute(status -> writer.write(message, null));
    }

    private List<ConsumerRecord<String, String>> records() {
        return KafkaTopicReader.readKey(kafkaAdmin.getConfigurationProperties(), Topics.INVENTORY_COMMANDS, orderId.toString());
    }

    private int pendingRows() {
        return jdbc.sql("SELECT count(*) FROM outbox WHERE published_at IS NULL").query(Integer.class).single();
    }
}
