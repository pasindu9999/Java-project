package io.github.pasindu9999.orderflow.inventory.messaging;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.pasindu9999.orderflow.contracts.inventory.ReserveInventory;
import io.github.pasindu9999.orderflow.inventory.TestcontainersConfiguration;
import io.github.pasindu9999.orderflow.inventory.app.ReservationService;
import io.github.pasindu9999.orderflow.inventory.domain.ReservationStatus;
import io.github.pasindu9999.orderflow.inventory.domain.StockLevel;
import io.github.pasindu9999.orderflow.inventory.persistence.ReservationRepository;
import io.github.pasindu9999.orderflow.inventory.persistence.StockRepository;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.github.pasindu9999.orderflow.messaging.inbox.IdempotentMessageHandler;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Concurrent reservations, driven through the inbox handler directly. Going through Kafka would serialise orders
 * that share a partition, so the row locks would rarely be contended.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class InventoryConcurrencyIT {

    static final int ORDERS = 10;

    @Autowired IdempotentMessageHandler inbox;
    @Autowired ReservationService reservationService;
    @Autowired MessageCodec codec;
    @Autowired JdbcClient jdbc;
    @Autowired StockRepository stock;
    @Autowired ReservationRepository reservations;

    @Test
    void shouldReserveExactlyTheAvailableUnitsWithoutDeadlock_whenOrdersRaceForTheLastUnits() throws Exception {
        String lamp = "LAMP-" + UUID.randomUUID();
        String cable = "CABLE-" + UUID.randomUUID();
        createStock(lamp, 3);
        createStock(cable, 100);
        List<UUID> orderIds = IntStream.range(0, ORDERS).mapToObj(i -> UUID.randomUUID()).toList();

        CountDownLatch start = new CountDownLatch(1);
        try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Boolean>> results = IntStream.range(0, ORDERS).mapToObj(i -> {
                // Half the orders list the SKUs in the opposite order: sorted locking must still avoid a deadlock.
                List<ReserveInventory.Line> lines = i % 2 == 0
                        ? List.of(new ReserveInventory.Line(lamp, 1), new ReserveInventory.Line(cable, 1))
                        : List.of(new ReserveInventory.Line(cable, 1), new ReserveInventory.Line(lamp, 1));
                String record = codec.encode(codec.wrap(new ReserveInventory(orderIds.get(i), lines), null));
                return CompletableFuture.supplyAsync(() -> {
                    awaitQuietly(start);
                    return inbox.handle(InventoryCommandListener.CONSUMER, record, reservationService::handle);
                }, threads);
            }).toList();

            start.countDown();
            for (CompletableFuture<Boolean> result : results) {
                assertThat(result.get(30, SECONDS)).as("every order is processed, none fails").isTrue();
            }
        }

        Map<ReservationStatus, Long> outcomes = orderIds.stream()
                .map(id -> reservations.findStatus(id).orElseThrow())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        assertThat(outcomes).containsExactlyInAnyOrderEntriesOf(Map.of(
                ReservationStatus.RESERVED, 3L,
                ReservationStatus.REJECTED, (long) ORDERS - 3));
        assertThat(stock.find(lamp)).contains(new StockLevel(lamp, 0, 3));
        assertThat(stock.find(cable)).as("rejected orders reserved nothing").contains(new StockLevel(cable, 97, 3));
    }

    private void createStock(String sku, int available) {
        jdbc.sql("INSERT INTO product_stock (sku, available, reserved) VALUES (:sku, :available, 0)")
                .param("sku", sku)
                .param("available", available)
                .update();
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
