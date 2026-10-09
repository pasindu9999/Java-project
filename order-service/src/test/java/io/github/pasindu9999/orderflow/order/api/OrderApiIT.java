package io.github.pasindu9999.orderflow.order.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pasindu9999.orderflow.order.TestcontainersConfiguration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class OrderApiIT {

    @Value("${local.server.port}")
    int port;

    @Autowired
    JdbcClient jdbc;

    final JsonMapper json = new JsonMapper();
    RestClient http;
    UUID customerId;
    String idempotencyKey;

    @BeforeEach
    void setUp() {
        http = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .defaultStatusHandler(status -> true, (request, response) -> { }) // assert on status ourselves
                .build();
        customerId = UUID.randomUUID();
        idempotencyKey = UUID.randomUUID().toString();
    }

    @Test
    void shouldAcceptOrderAsPending_whenRequestIsValid() {
        ResponseEntity<String> created = post(idempotencyKey, orderJson(customerId, "12.50"));

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        JsonNode body = body(created);
        String orderId = body.get("orderId").asString();
        assertThat(body.get("status").asString()).isEqualTo("PENDING");
        assertThat(created.getHeaders().getLocation()).hasPath("/orders/" + orderId);
        assertThat(created.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("false");

        ResponseEntity<String> fetched = get("/orders/" + orderId);
        assertThat(fetched.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode order = body(fetched);
        assertThat(order.get("status").asString()).isEqualTo("PENDING");
        assertThat(order.get("customerId").asString()).isEqualTo(customerId.toString());
        assertThat(order.get("totalAmount").asString()).isEqualTo("37.50");
        assertThat(order.get("currency").asString()).isEqualTo("EUR");
        assertThat(order.get("lines")).hasSize(2);
        assertThat(order.get("lines").get(0).get("unitPrice").asString()).isEqualTo("12.50");
    }

    @Test
    void shouldSetSagaDeadline_whenOrderIsPlaced() {
        String orderId = body(post(idempotencyKey, orderJson(customerId, "12.50"))).get("orderId").asString();

        Integer seconds = jdbc.sql("SELECT EXTRACT(EPOCH FROM deadline_at - created_at)::int FROM orders WHERE id = :id")
                .param("id", UUID.fromString(orderId))
                .query(Integer.class)
                .single();
        assertThat(seconds).isEqualTo(30);
    }

    @Test
    void shouldReturnSameOrder_whenSameKeyAndEquivalentBodyAreRetried() {
        String first = body(post(idempotencyKey, orderJson(customerId, "12.50"))).get("orderId").asString();

        ResponseEntity<String> retry = post(idempotencyKey, orderJson(customerId, "12.5")); // same meaning, other formatting

        assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(body(retry).get("orderId").asString()).isEqualTo(first);
        assertThat(retry.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(countOrders(customerId)).isEqualTo(1);
    }

    @Test
    void shouldReturn422_whenSameKeyIsReusedWithDifferentBody() {
        post(idempotencyKey, orderJson(customerId, "12.50"));

        ResponseEntity<String> reuse = post(idempotencyKey, orderJson(customerId, "99.00"));

        assertThat(reuse.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_CONTENT);
        assertThat(reuse.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(body(reuse).get("title").asString()).isEqualTo("Idempotency key reused");
        assertThat(countOrders(customerId)).isEqualTo(1);
    }

    @Test
    void shouldCreateSeparateOrders_whenDifferentCustomersUseSameKey() {
        UUID otherCustomer = UUID.randomUUID();

        String first = body(post(idempotencyKey, orderJson(customerId, "12.50"))).get("orderId").asString();
        String second = body(post(idempotencyKey, orderJson(otherCustomer, "12.50"))).get("orderId").asString();

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void shouldCreateExactlyOneOrder_whenSameKeyIsPostedConcurrently() throws Exception {
        int clients = 10;
        String body = orderJson(customerId, "12.50");
        CountDownLatch startTogether = new CountDownLatch(1);

        List<ResponseEntity<String>> responses;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<ResponseEntity<String>>> futures = IntStream.range(0, clients)
                    .mapToObj(i -> executor.submit(() -> {
                        startTogether.await();
                        return post(idempotencyKey, body);
                    }))
                    .toList();
            startTogether.countDown();
            responses = futures.stream().map(OrderApiIT::join).toList();
        }

        assertThat(responses).allSatisfy(r -> assertThat(r.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED));
        List<String> orderIds = responses.stream().map(r -> body(r).get("orderId").asString()).distinct().toList();
        assertThat(orderIds).hasSize(1);
        assertThat(countOrders(customerId)).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM outbox WHERE message_key = :key")
                .param("key", orderIds.getFirst())
                .query(Integer.class)
                .single()).as("losing requests rolled back their command too").isEqualTo(1);
    }

    @Test
    void shouldReturn400_whenIdempotencyKeyHeaderIsMissing() {
        ResponseEntity<String> response = post(null, orderJson(customerId, "12.50"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(countOrders(customerId)).isZero();
    }

    @Test
    void shouldReturn400WithEveryViolation_whenBodyBreaksBusinessRules() {
        ResponseEntity<String> response = post(idempotencyKey, """
                { "currency": "XYZ",
                  "lines": [ { "sku": "MUG-RED", "quantity": 0, "unitPrice": "-1" } ] }
                """);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        JsonNode problem = body(response);
        assertThat(problem.get("title").asString()).isEqualTo("Invalid order");
        assertThat(problem.get("errors").valueStream().map(JsonNode::asString).toList()).containsExactly(
                "customerId is required",
                "currency must be an ISO 4217 code such as EUR, got 'XYZ'",
                "lines[0].quantity must be between 1 and 1000",
                "lines[0].unitPrice must not be negative");
    }

    @Test
    void shouldReturn400_whenJsonIsMalformed() {
        ResponseEntity<String> response = post(idempotencyKey, "{ \"customerId\": ");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getHeaders().getContentType()).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    }

    @Test
    void shouldReturn404_whenOrderDoesNotExist() {
        ResponseEntity<String> response = get("/orders/" + UUID.randomUUID());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(body(response).get("title").asString()).isEqualTo("Order not found");
    }

    @Test
    void shouldReturn400_whenOrderIdIsNotAUuid() {
        assertThat(get("/orders/not-a-uuid").getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private ResponseEntity<String> post(String key, String body) {
        var request = http.post().uri("/orders").contentType(MediaType.APPLICATION_JSON);
        if (key != null) {
            request = request.header("Idempotency-Key", key);
        }
        return request.body(body).retrieve().toEntity(String.class);
    }

    private ResponseEntity<String> get(String path) {
        return http.get().uri(path).retrieve().toEntity(String.class);
    }

    private JsonNode body(ResponseEntity<String> response) {
        return json.readTree(response.getBody());
    }

    private int countOrders(UUID customer) {
        return jdbc.sql("SELECT count(*) FROM orders WHERE customer_id = :customerId")
                .param("customerId", customer)
                .query(Integer.class)
                .single();
    }

    /** Two lines: 2 x MUG-RED and 1 x TEA-GREEN at the given unit price. */
    private static String orderJson(UUID customerId, String unitPrice) {
        return """
                { "customerId": "%s", "currency": "EUR",
                  "lines": [ { "sku": "MUG-RED", "quantity": 2, "unitPrice": "%s" },
                             { "sku": "TEA-GREEN", "quantity": 1, "unitPrice": "%s" } ] }
                """.formatted(customerId, unitPrice, unitPrice);
    }

    private static <T> T join(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
