package io.github.pasindu9999.orderflow.e2e;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

/**
 * The whole saga across the three real services, driven only through their public HTTP APIs and Kafka. Each test
 * creates its own SKUs and customer, so tests don't depend on each other or on seed data.
 */
class OrderSagaE2eIT {

    record PlacedOrder(UUID orderId, String status) {
    }

    record OrderView(String status, String cancelReason, String totalAmount) {
    }

    record StockView(int available, int reserved) {
    }

    record PaymentView(String status, String failureReason, String amount) {
    }

    static OrderflowSystem system;

    final UUID customerId = UUID.randomUUID();
    final String mug = "MUG-" + UUID.randomUUID();
    final String tea = "TEA-" + UUID.randomUUID();
    RestClient http;

    @BeforeAll
    static void startSystem() {
        system = OrderflowSystem.start();
    }

    @AfterAll
    static void stopSystem() {
        if (system != null) {
            system.close();
        }
    }

    @BeforeEach
    void setUp() {
        http = RestClient.builder().defaultStatusHandler(status -> true, (request, response) -> { }).build();
    }

    @Test
    void shouldConfirmOrderChargeCustomerAndKeepStockReserved_whenStockAndPaymentAreFine() {
        createStock(mug, 10);
        createStock(tea, 5);

        UUID orderId = placeOrder("""
                [ { "sku": "%s", "quantity": 2, "unitPrice": "12.50" },
                  { "sku": "%s", "quantity": 1, "unitPrice": "8.00" } ]""".formatted(mug, tea));

        OrderView order = awaitTerminal(orderId);
        assertThat(order.status()).isEqualTo("CONFIRMED");
        assertThat(order.cancelReason()).isNull();
        assertThat(payment(orderId).getBody()).isEqualTo(new PaymentView("SUCCEEDED", null, "33.00"));
        assertThat(stock(mug)).isEqualTo(new StockView(8, 2));
        assertThat(stock(tea)).isEqualTo(new StockView(4, 1));
    }

    @Test
    void shouldCancelWithoutReservingOrCharging_whenOneLineIsOutOfStock() {
        createStock(mug, 10);
        createStock(tea, 1);

        UUID orderId = placeOrder("""
                [ { "sku": "%s", "quantity": 2, "unitPrice": "12.50" },
                  { "sku": "%s", "quantity": 2, "unitPrice": "8.00" } ]""".formatted(mug, tea));

        OrderView order = awaitTerminal(orderId);
        assertThat(order.status()).isEqualTo("CANCELLED");
        assertThat(order.cancelReason()).isEqualTo("OUT_OF_STOCK");
        assertThat(payment(orderId).getStatusCode()).as("never charged").isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(stock(mug)).isEqualTo(new StockView(10, 0));
        assertThat(stock(tea)).isEqualTo(new StockView(1, 0));
    }

    @Test
    void shouldCancelAndReturnStockToItsInitialLevel_whenPaymentIsDeclined() {
        createStock(mug, 10);

        UUID orderId = placeOrder("""
                [ { "sku": "%s", "quantity": 3, "unitPrice": "500.00" } ]""".formatted(mug)); // 1500.00 > limit

        OrderView order = awaitTerminal(orderId);
        assertThat(order.status()).isEqualTo("CANCELLED");
        assertThat(order.cancelReason()).isEqualTo("PAYMENT_DECLINED");
        assertThat(payment(orderId).getBody()).isEqualTo(new PaymentView("FAILED", "DECLINED_LIMIT", "1500.00"));
        // The order is cancelled first; the ReleaseInventory compensation lands a moment later.
        await().atMost(30, SECONDS).untilAsserted(() -> assertThat(stock(mug)).isEqualTo(new StockView(10, 0)));
    }

    private UUID placeOrder(String linesJson) {
        ResponseEntity<PlacedOrder> response = http.post().uri(system.orderUrl() + "/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .body("""
                        { "customerId": "%s", "currency": "EUR", "lines": %s }""".formatted(customerId, linesJson))
                .retrieve()
                .toEntity(PlacedOrder.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return response.getBody().orderId();
    }

    /** Polls the public API until the saga finishes; 30 s is the saga's own deadline. */
    private OrderView awaitTerminal(UUID orderId) {
        return await().atMost(30, SECONDS).until(
                () -> http.get().uri(system.orderUrl() + "/orders/{id}", orderId).retrieve().body(OrderView.class),
                order -> order.status().equals("CONFIRMED") || order.status().equals("CANCELLED"));
    }

    private ResponseEntity<PaymentView> payment(UUID orderId) {
        return http.get().uri(system.paymentUrl() + "/payments?orderId={id}", orderId)
                .retrieve().toEntity(PaymentView.class);
    }

    private StockView stock(String sku) {
        return http.get().uri(system.inventoryUrl() + "/stock/{sku}", sku).retrieve().body(StockView.class);
    }

    private void createStock(String sku, int available) {
        system.inventoryDb().sql("INSERT INTO product_stock (sku, available, reserved) VALUES (:sku, :available, 0)")
                .param("sku", sku)
                .param("available", available)
                .update();
    }
}
