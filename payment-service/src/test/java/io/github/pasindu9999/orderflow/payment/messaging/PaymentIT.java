package io.github.pasindu9999.orderflow.payment.messaging;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.pasindu9999.orderflow.contracts.Message;
import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentFailed;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentRefunded;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentSucceeded;
import io.github.pasindu9999.orderflow.contracts.payment.ProcessPayment;
import io.github.pasindu9999.orderflow.contracts.payment.RefundPayment;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.github.pasindu9999.orderflow.messaging.error.DeadLetterErrorHandler;
import io.github.pasindu9999.orderflow.messaging.testing.KafkaTopicReader;
import io.github.pasindu9999.orderflow.payment.TestcontainersConfiguration;
import io.github.pasindu9999.orderflow.payment.domain.DeclineReason;
import io.github.pasindu9999.orderflow.payment.domain.Payment;
import io.github.pasindu9999.orderflow.payment.domain.PaymentStatus;
import io.github.pasindu9999.orderflow.payment.persistence.PaymentRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.client.RestClient;

/**
 * The test plays order-service: it publishes commands to {@code payment.commands} and reads the replies on
 * {@code payment.events}. Every test uses its own order and customer.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class PaymentIT {

    @Value("${local.server.port}")
    int port;

    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired MessageCodec codec;
    @Autowired JdbcClient jdbc;
    @Autowired PaymentRepository payments;
    @Autowired MeterRegistry meters;

    final UUID orderId = UUID.randomUUID();
    final UUID customerId = UUID.randomUUID();
    RestClient http;

    @BeforeEach
    void setUp() {
        http = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .defaultStatusHandler(status -> true, (request, response) -> { })
                .build();
    }

    @Test
    void shouldChargeAndReplySucceeded_whenAmountIsWithinLimit() {
        Envelope command = send(processPayment("37.50"));

        Envelope reply = awaitReplies(1).getFirst();
        Payment payment = payments.findByOrderId(orderId).orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(reply.payload()).isEqualTo(new PaymentSucceeded(orderId, payment.id(), new BigDecimal("37.50"), "EUR"));
        assertThat(reply.causationId()).isEqualTo(command.messageId());

        ResponseEntity<String> response = getPayment();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"status\":\"SUCCEEDED\"", "\"amount\":\"37.50\"");
    }

    @Test
    void shouldChargeOnceAfterRetries_whenProviderIsUnavailableTwiceForTheFlakyAmount() {
        long sentAt = System.nanoTime();
        send(processPayment("13.13"));

        Envelope reply = awaitReplies(1).getFirst();
        assertThat(reply.payload()).isInstanceOf(PaymentSucceeded.class);
        assertThat(Duration.ofNanos(System.nanoTime() - sentAt))
                .as("two failed attempts, then 500 ms + 1 s of backoff").isGreaterThanOrEqualTo(Duration.ofMillis(1500));
        assertThat(paymentRows()).isEqualTo(1);
        assertThat(KafkaTopicReader.readKey(kafkaAdmin.getConfigurationProperties(),
                Topics.PAYMENT_COMMANDS + DeadLetterErrorHandler.DLT_SUFFIX, orderId.toString())).isEmpty();
    }

    @Test
    void shouldDeclineAndRecordFailedPayment_whenAmountIsAboveLimit() {
        send(processPayment("1500.00"));

        assertThat(awaitReplies(1).getFirst().payload())
                .isEqualTo(new PaymentFailed(orderId, PaymentFailed.Reason.DECLINED_LIMIT));
        Payment payment = payments.findByOrderId(orderId).orElseThrow();
        assertThat(payment.status()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.failureReason()).isEqualTo(DeclineReason.DECLINED_LIMIT);
    }

    @Test
    void shouldChargeOnce_whenSameCommandIsDeliveredTwice() {
        double duplicatesBefore = duplicatesSkipped();
        String record = codec.encode(codec.wrap(processPayment("20.00"), null));

        sendRaw(record);
        sendRaw(record); // e.g. order-service's relay crashed after sending, before marking the row

        await().atMost(15, SECONDS).until(() -> duplicatesSkipped() >= duplicatesBefore + 1);
        assertThat(awaitReplies(1).getFirst().payload()).isInstanceOf(PaymentSucceeded.class);
        assertThat(paymentRows()).isEqualTo(1);
    }

    @Test
    void shouldKeepFirstOutcome_whenADifferentCommandArrivesForTheSameOrder() {
        send(processPayment("20.00"));
        awaitReplies(1);

        // New messageId, so the inbox can't catch it; the one-payment-per-order key does.
        Envelope second = send(processPayment("20.00"));
        await().atMost(15, SECONDS).until(() -> processed(second));

        assertThat(replies()).hasSize(1);
        assertThat(paymentRows()).isEqualTo(1);
    }

    @Test
    void shouldRefundOnce_whenSucceededPaymentIsRefundedTwice() {
        send(processPayment("20.00"));
        awaitReplies(1);
        UUID paymentId = payments.findByOrderId(orderId).orElseThrow().id();

        Envelope first = send(new RefundPayment(orderId, RefundPayment.Reason.LATE_PAYMENT));
        Envelope second = send(new RefundPayment(orderId, RefundPayment.Reason.LATE_PAYMENT)); // new messageId
        await().atMost(15, SECONDS).until(() -> processed(first) && processed(second));

        assertThat(payments.findByOrderId(orderId).orElseThrow().status()).isEqualTo(PaymentStatus.REFUNDED);
        List<Envelope> replies = awaitReplies(2);
        assertThat(replies.get(1).payload()).isEqualTo(new PaymentRefunded(orderId, paymentId));
        assertThat(replies.get(1).causationId()).isEqualTo(first.messageId());
    }

    @Test
    void shouldDoNothing_whenRefundingAnOrderThatWasNeverCharged() {
        Envelope refund = send(new RefundPayment(orderId, RefundPayment.Reason.LATE_PAYMENT));

        await().atMost(15, SECONDS).until(() -> processed(refund));
        assertThat(paymentRows()).isZero();
        assertThat(replies()).isEmpty();
    }

    @Test
    void shouldReturn404Problem_whenOrderHasNoPayment() {
        ResponseEntity<String> response = getPayment();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody()).contains(orderId.toString());
    }

    private ProcessPayment processPayment(String amount) {
        return new ProcessPayment(orderId, customerId, new BigDecimal(amount), "EUR", Instant.now().plusSeconds(30));
    }

    private Envelope send(Message command) {
        Envelope envelope = codec.wrap(command, null);
        sendRaw(codec.encode(envelope));
        return envelope;
    }

    private void sendRaw(String record) {
        kafka.send(Topics.PAYMENT_COMMANDS, orderId.toString(), record).join();
    }

    /** Waits for the expected number of replies, then checks no extra one arrived with them. */
    private List<Envelope> awaitReplies(int expected) {
        List<Envelope> replies = await().atMost(15, SECONDS).until(this::replies, r -> r.size() >= expected);
        assertThat(replies).hasSize(expected);
        return replies;
    }

    private List<Envelope> replies() {
        return KafkaTopicReader.readKey(kafkaAdmin.getConfigurationProperties(), Topics.PAYMENT_EVENTS, orderId.toString())
                .stream().map(r -> codec.decode(r.value())).toList();
    }

    private boolean processed(Envelope envelope) {
        return jdbc.sql("SELECT count(*) = 1 FROM processed_message WHERE consumer = :consumer AND message_id = :id")
                .param("consumer", PaymentCommandListener.CONSUMER)
                .param("id", envelope.messageId())
                .query(Boolean.class)
                .single();
    }

    private int paymentRows() {
        return jdbc.sql("SELECT count(*) FROM payment WHERE order_id = :orderId")
                .param("orderId", orderId)
                .query(Integer.class)
                .single();
    }

    private double duplicatesSkipped() {
        var counter = meters.find("messaging.duplicates.skipped").tag("consumer", PaymentCommandListener.CONSUMER).counter();
        return counter == null ? 0 : counter.count();
    }

    private ResponseEntity<String> getPayment() {
        return http.get().uri("/payments?orderId={orderId}", orderId).retrieve().toEntity(String.class);
    }
}
