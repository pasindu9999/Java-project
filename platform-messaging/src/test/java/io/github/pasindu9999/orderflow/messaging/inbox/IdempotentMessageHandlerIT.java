package io.github.pasindu9999.orderflow.messaging.inbox;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.pasindu9999.orderflow.contracts.inventory.InventoryReserved;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.github.pasindu9999.orderflow.messaging.MessageParseException;
import io.github.pasindu9999.orderflow.messaging.outbox.MessagingTestApplication;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

@SpringBootTest(classes = MessagingTestApplication.class)
class IdempotentMessageHandlerIT {

    static final String CONSUMER = "test-handler";

    @Autowired IdempotentMessageHandler inbox;
    @Autowired MessageCodec codec;
    @Autowired JdbcClient jdbc;
    @Autowired MeterRegistry meters;

    final AtomicInteger handlerRuns = new AtomicInteger();
    String record;

    @BeforeEach
    void newMessage() {
        record = codec.encode(codec.wrap(new InventoryReserved(UUID.randomUUID()), null));
    }

    @Test
    void shouldRunHandlerOnce_whenSameMessageIsDeliveredTwice() {
        double duplicatesBefore = duplicateCount();

        assertThat(inbox.handle(CONSUMER, record, envelope -> handlerRuns.incrementAndGet())).isTrue();
        assertThat(inbox.handle(CONSUMER, record, envelope -> handlerRuns.incrementAndGet())).isFalse();

        assertThat(handlerRuns).hasValue(1);
        assertThat(duplicateCount()).isEqualTo(duplicatesBefore + 1);
    }

    @Test
    void shouldProcessAgain_whenHandlerFailedTheFirstTime() {
        assertThatThrownBy(() -> inbox.handle(CONSUMER, record, envelope -> {
            throw new IllegalStateException("database hiccup");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(inboxRows()).as("the inbox row rolled back with the failed handler").isZero();
        assertThat(inbox.handle(CONSUMER, record, envelope -> handlerRuns.incrementAndGet())).isTrue();
        assertThat(handlerRuns).hasValue(1);
    }

    @Test
    void shouldProcessOncePerConsumer_whenTwoConsumersReceiveSameMessage() {
        inbox.handle("consumer-a", record, envelope -> handlerRuns.incrementAndGet());
        inbox.handle("consumer-b", record, envelope -> handlerRuns.incrementAndGet());

        assertThat(handlerRuns).hasValue(2);
    }

    @Test
    void shouldRunHandlerOnce_whenDuplicatesArriveConcurrently() throws Exception {
        CountDownLatch firstIsInsideHandler = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);

        // First delivery: inserts the inbox row, then holds its transaction open inside the handler.
        CompletableFuture<Boolean> first = CompletableFuture.supplyAsync(() -> inbox.handle(CONSUMER, record, envelope -> {
            handlerRuns.incrementAndGet();
            firstIsInsideHandler.countDown();
            await().atMost(10, SECONDS).until(() -> releaseFirst.getCount() == 0);
        }));
        assertThat(firstIsInsideHandler.await(10, SECONDS)).isTrue();

        // Second delivery: its INSERT must block on the primary key held by the first transaction.
        CompletableFuture<Boolean> second = CompletableFuture.supplyAsync(
                () -> inbox.handle(CONSUMER, record, envelope -> handlerRuns.incrementAndGet()));
        await().atMost(10, SECONDS).until(this::someSessionIsWaitingForALock);

        releaseFirst.countDown();

        assertThat(first.get(10, SECONDS)).isTrue();
        assertThat(second.get(10, SECONDS)).as("second saw the committed row and skipped").isFalse();
        assertThat(handlerRuns).hasValue(1);
    }

    @Test
    void shouldThrowParseExceptionWithoutRecordingAnything_whenRecordIsNotAnEnvelope() {
        assertThatThrownBy(() -> inbox.handle(CONSUMER, "{ not json", envelope -> handlerRuns.incrementAndGet()))
                .isInstanceOf(MessageParseException.class);

        assertThat(handlerRuns).hasValue(0);
    }

    private boolean someSessionIsWaitingForALock() {
        return jdbc.sql("SELECT count(*) > 0 FROM pg_stat_activity WHERE wait_event_type = 'Lock'")
                .query(Boolean.class)
                .single();
    }

    private int inboxRows() {
        Envelope envelope = codec.decode(record);
        return jdbc.sql("SELECT count(*) FROM processed_message WHERE message_id = :id")
                .param("id", envelope.messageId())
                .query(Integer.class)
                .single();
    }

    private double duplicateCount() {
        var counter = meters.find("messaging.duplicates.skipped").tag("consumer", CONSUMER).counter();
        return counter == null ? 0 : counter.count();
    }
}
