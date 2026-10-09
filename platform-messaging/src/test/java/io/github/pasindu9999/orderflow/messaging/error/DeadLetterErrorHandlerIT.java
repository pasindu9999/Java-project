package io.github.pasindu9999.orderflow.messaging.error;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.pasindu9999.orderflow.contracts.inventory.InventoryReserved;
import io.github.pasindu9999.orderflow.messaging.FaultInjector;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.github.pasindu9999.orderflow.messaging.MessageParseException;
import io.github.pasindu9999.orderflow.messaging.NonRetryableMessageException;
import io.github.pasindu9999.orderflow.messaging.inbox.IdempotentMessageHandler;
import io.github.pasindu9999.orderflow.messaging.outbox.MessagingTestApplication;
import io.github.pasindu9999.orderflow.messaging.testing.KafkaTopicReader;
import io.github.pasindu9999.orderflow.messaging.testing.ProgrammableFaultInjector;
import io.github.pasindu9999.orderflow.messaging.testing.ProgrammableFaultInjector.InjectedFault;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * The shared consumer error handler (ADR-0004), against a test listener that goes through the real inbox. Each
 * test uses its own key, which keeps all its records on one partition, so "the next record is still processed"
 * really means the partition wasn't blocked.
 */
@SpringBootTest(classes = MessagingTestApplication.class, properties = {
        "spring.kafka.consumer.group-id=error-handling-test",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.consumer.enable-auto-commit=false",
        "spring.kafka.listener.ack-mode=record"})
@Import(DeadLetterErrorHandlerIT.Config.class)
class DeadLetterErrorHandlerIT {

    static final String TOPIC = "test.commands";
    static final String DLT = TOPIC + DeadLetterErrorHandler.DLT_SUFFIX;

    @TestConfiguration(proxyBeanMethods = false)
    static class Config {

        @Bean
        NewTopic testCommands() {
            return TopicBuilder.name(TOPIC).partitions(3).replicas(1).build();
        }

        @Bean
        NewTopic testCommandsDlt() {
            return TopicBuilder.name(DLT).partitions(3).replicas(1).build();
        }

        @Bean
        TestListener testListener(IdempotentMessageHandler inbox, FaultInjector faults) {
            return new TestListener(inbox, faults);
        }
    }

    /** Counts attempts and successful (committed) handler runs per order. */
    static class TestListener {

        static final String HANDLE = "TestListener.HANDLE";

        final Map<UUID, AtomicInteger> attempts = new ConcurrentHashMap<>();
        final Map<UUID, AtomicInteger> effects = new ConcurrentHashMap<>();
        final Map<UUID, Long> succeededAt = new ConcurrentHashMap<>();
        final Set<UUID> rejected = ConcurrentHashMap.newKeySet();
        private final IdempotentMessageHandler inbox;
        private final FaultInjector faults;

        TestListener(IdempotentMessageHandler inbox, FaultInjector faults) {
            this.inbox = inbox;
            this.faults = faults;
        }

        @KafkaListener(topics = TOPIC)
        void onRecord(String record) {
            inbox.handle("test-commands-handler", record, envelope -> {
                UUID orderId = envelope.correlationId();
                attempts.computeIfAbsent(orderId, id -> new AtomicInteger()).incrementAndGet();
                if (rejected.contains(orderId)) {
                    throw new NonRetryableMessageException("Rejected by the test for order " + orderId);
                }
                faults.at(HANDLE);
                effects.computeIfAbsent(orderId, id -> new AtomicInteger()).incrementAndGet();
                succeededAt.put(orderId, System.nanoTime());
            });
        }
    }

    @Autowired TestListener listener;
    @Autowired ProgrammableFaultInjector faults;
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired MessageCodec codec;
    @Autowired MeterRegistry meters;

    final String key = "key-" + UUID.randomUUID();

    @AfterEach
    void disarmFaults() {
        faults.reset();
    }

    @Test
    void shouldProcessOnceOnThirdAttemptAfterBackingOff_whenHandlerFailsTwice() {
        faults.failTimes(TestListener.HANDLE, 2);
        UUID orderId = UUID.randomUUID();

        long sentAt = System.nanoTime();
        send(orderId);

        await().atMost(15, SECONDS).until(() -> effectsFor(orderId) == 1);
        assertThat(listener.attempts.get(orderId)).hasValue(3);
        assertThat(Duration.ofNanos(listener.succeededAt.get(orderId) - sentAt))
                .as("500 ms + 1 s of backoff before the third attempt")
                .isGreaterThanOrEqualTo(Duration.ofMillis(1500));
        assertThat(deadLetters()).isEmpty();
    }

    @Test
    void shouldDeadLetterAfterRetryBudgetAndKeepConsuming_whenHandlerKeepsFailing() {
        double dltBefore = dltCount();
        faults.failTimes(TestListener.HANDLE, 100);
        UUID failing = UUID.randomUUID();

        int partition = send(failing);

        ConsumerRecord<String, String> parked = awaitSingleDeadLetter();
        assertThat(listener.attempts.get(failing)).as("first attempt + 3 retries").hasValue(4);
        assertThat(effectsFor(failing)).isZero();
        assertThat(parked.partition()).isEqualTo(partition);
        assertThat(header(parked, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(TOPIC);
        assertThat(exceptionClasses(parked)).contains(InjectedFault.class.getName());
        assertThat(dltCount()).isEqualTo(dltBefore + 1);

        faults.reset();
        UUID next = UUID.randomUUID();
        send(next);
        await().atMost(15, SECONDS).until(() -> effectsFor(next) == 1);
    }

    @Test
    void shouldDeadLetterImmediatelyAndKeepConsuming_whenRecordIsNotAnEnvelope() {
        int partition = kafka.send(TOPIC, key, "{ not json").join().getRecordMetadata().partition();
        UUID next = UUID.randomUUID();
        send(next);

        await().atMost(15, SECONDS).until(() -> effectsFor(next) == 1);
        ConsumerRecord<String, String> parked = awaitSingleDeadLetter();
        assertThat(parked.value()).as("kept as is, so it can be replayed").isEqualTo("{ not json");
        assertThat(parked.partition()).isEqualTo(partition);
        assertThat(header(parked, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(TOPIC);
        assertThat(parked.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)).isNotNull();
        assertThat(exceptionClasses(parked)).contains(MessageParseException.class.getName());
    }

    @Test
    void shouldDeadLetterWithoutRetrying_whenHandlerRejectsPayload() {
        UUID invalid = UUID.randomUUID();
        listener.rejected.add(invalid);

        send(invalid);

        ConsumerRecord<String, String> parked = awaitSingleDeadLetter();
        assertThat(listener.attempts.get(invalid)).as("non-retryable: one attempt only").hasValue(1);
        assertThat(exceptionClasses(parked)).contains(NonRetryableMessageException.class.getName());
    }

    /** @return the partition the record landed on */
    private int send(UUID orderId) {
        String record = codec.encode(codec.wrap(new InventoryReserved(orderId), null));
        return kafka.send(TOPIC, key, record).join().getRecordMetadata().partition();
    }

    private ConsumerRecord<String, String> awaitSingleDeadLetter() {
        List<ConsumerRecord<String, String>> parked = await().atMost(15, SECONDS).until(this::deadLetters, r -> !r.isEmpty());
        assertThat(parked).hasSize(1);
        return parked.getFirst();
    }

    private List<ConsumerRecord<String, String>> deadLetters() {
        return KafkaTopicReader.readKey(kafkaAdmin.getConfigurationProperties(), DLT, key);
    }

    private int effectsFor(UUID orderId) {
        AtomicInteger effects = listener.effects.get(orderId);
        return effects == null ? 0 : effects.get();
    }

    /** The exception headers: the listener's wrapper exception and its cause. */
    private static List<String> exceptionClasses(ConsumerRecord<String, String> record) {
        return List.of(header(record, KafkaHeaders.DLT_EXCEPTION_FQCN), header(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN));
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }

    private double dltCount() {
        var counter = meters.find("messaging.dlt.published").tag("topic", TOPIC).counter();
        return counter == null ? 0 : counter.count();
    }
}
