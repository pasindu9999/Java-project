package io.github.pasindu9999.orderflow.order.api;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.contracts.inventory.ReserveInventory;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.github.pasindu9999.orderflow.messaging.outbox.OutboxWriter;
import io.github.pasindu9999.orderflow.messaging.testing.KafkaTopicReader;
import io.github.pasindu9999.orderflow.messaging.testing.ProgrammableFaultInjector;
import io.github.pasindu9999.orderflow.order.TestcontainersConfiguration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

/** Placing an order and emitting the saga's first command are one atomic step (ADR-0002). */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class OrderOutboxIT {

    @Value("${local.server.port}")
    int port;

    @Autowired JdbcClient jdbc;
    @Autowired KafkaAdmin kafkaAdmin;
    @Autowired MessageCodec codec;
    @Autowired ProgrammableFaultInjector faults;

    final JsonMapper json = new JsonMapper();
    RestClient http;
    UUID customerId;

    @BeforeEach
    void setUp() {
        http = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .defaultStatusHandler(status -> true, (request, response) -> { })
                .build();
        customerId = UUID.randomUUID();
    }

    @AfterEach
    void disarmFaults() {
        faults.reset();
    }

    @Test
    void shouldPublishReserveInventory_whenOrderIsPlaced() {
        UUID orderId = placeOrder();

        List<ConsumerRecord<String, String>> records = await().atMost(10, SECONDS)
                .until(() -> commandsFor(orderId), r -> !r.isEmpty());

        assertThat(records).hasSize(1);
        Envelope envelope = codec.decode(records.getFirst().value());
        assertThat(envelope.messageType()).isEqualTo("ReserveInventory");
        assertThat(envelope.correlationId()).isEqualTo(orderId);
        assertThat(envelope.causationId()).as("first command of the saga").isNull();
        assertThat(envelope.producer()).isEqualTo("order-service");
        assertThat(envelope.payload()).isEqualTo(new ReserveInventory(orderId, List.of(
                new ReserveInventory.Line("MUG-RED", 2),
                new ReserveInventory.Line("TEA-GREEN", 1))));

        await().atMost(5, SECONDS).untilAsserted(() -> assertThat(jdbc
                .sql("SELECT published_at IS NOT NULL FROM outbox WHERE message_key = :key")
                .param("key", orderId.toString())
                .query(Boolean.class)
                .single()).isTrue());
    }

    @Test
    void shouldStoreNeitherOrderNorCommand_whenTransactionFailsAfterOutboxWrite() {
        int outboxRowsBefore = countOutboxRows();
        faults.failOnce(OutboxWriter.AFTER_WRITE);

        ResponseEntity<String> response = post();

        assertThat(response.getStatusCode().is5xxServerError()).isTrue();
        assertThat(jdbc.sql("SELECT count(*) FROM orders WHERE customer_id = :c").param("c", customerId)
                .query(Integer.class).single()).isZero();
        assertThat(countOutboxRows()).isEqualTo(outboxRowsBefore);
    }

    private UUID placeOrder() {
        ResponseEntity<String> response = post();
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        return UUID.fromString(json.readTree(response.getBody()).get("orderId").asString());
    }

    private ResponseEntity<String> post() {
        return http.post().uri("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .body("""
                        { "customerId": "%s", "currency": "EUR",
                          "lines": [ { "sku": "MUG-RED", "quantity": 2, "unitPrice": "12.50" },
                                     { "sku": "TEA-GREEN", "quantity": 1, "unitPrice": "8.00" } ] }
                        """.formatted(customerId))
                .retrieve()
                .toEntity(String.class);
    }

    private List<ConsumerRecord<String, String>> commandsFor(UUID orderId) {
        return KafkaTopicReader.readKey(kafkaAdmin.getConfigurationProperties(), Topics.INVENTORY_COMMANDS, orderId.toString());
    }

    private int countOutboxRows() {
        return jdbc.sql("SELECT count(*) FROM outbox").query(Integer.class).single();
    }
}
