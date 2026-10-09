package io.github.pasindu9999.orderflow.order.app;

import io.github.pasindu9999.orderflow.contracts.inventory.InventoryEvent;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryRejected;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryReserved;
import io.github.pasindu9999.orderflow.contracts.payment.ProcessPayment;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.NonRetryableMessageException;
import io.github.pasindu9999.orderflow.messaging.outbox.OutboxWriter;
import io.github.pasindu9999.orderflow.order.domain.CancelReason;
import io.github.pasindu9999.orderflow.order.domain.Order;
import io.github.pasindu9999.orderflow.order.persistence.OrderRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * The saga orchestrator: applies the reply handling table (ARCHITECTURE §3.3) to each reply. The status change
 * and the next command go into the same transaction as the inbox row, so they commit together or not at all.
 *
 * <p>Covers the inventory rows of the table. Payment replies follow on Day 6. A late {@code InventoryReserved}
 * on a cancelled order is still ignored here; it can only happen once the timeout sweeper exists, and turning it
 * into {@code ReleaseInventory} is Day 9 (docs/PLAN.md).
 */
@Service
public class OrderSaga {

    private static final Logger log = LoggerFactory.getLogger(OrderSaga.class);

    private final OrderRepository orders;
    private final OutboxWriter outbox;
    private final Clock clock;

    public OrderSaga(OrderRepository orders, OutboxWriter outbox, Clock clock) {
        this.orders = orders;
        this.outbox = outbox;
        this.clock = clock;
    }

    /** MANDATORY: only ever called from the idempotent inbox handler, which owns the transaction. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void onInventoryEvent(Envelope envelope) {
        switch (envelope.payloadAs(InventoryEvent.class)) {
            case InventoryReserved event -> onReserved(load(event.orderId()), envelope.messageId());
            case InventoryRejected event -> onRejected(load(event.orderId()), event);
        }
    }

    private void onReserved(Order order, UUID causationId) {
        switch (order.status()) {
            case PENDING -> {
                Order awaiting = save(order.awaitPayment(now()));
                outbox.write(processPayment(awaiting), causationId);
                log.info("Order {} reserved; awaiting payment of {} {}", order.id(), order.totalAmount(), order.currency());
            }
            case AWAITING_PAYMENT, CONFIRMED, CANCELLED -> ignore(order, "InventoryReserved");
        }
    }

    private void onRejected(Order order, InventoryRejected event) {
        switch (order.status()) {
            case PENDING -> {
                save(order.cancel(CancelReason.OUT_OF_STOCK, now()));
                log.info("Order {} cancelled: inventory rejected it ({}, {})", order.id(), event.reason(), event.shortages());
            }
            case CANCELLED -> log.info("Order {} is already cancelled; recorded late InventoryRejected", order.id());
            case AWAITING_PAYMENT, CONFIRMED -> ignore(order, "InventoryRejected");
        }
    }

    private Order save(Order changed) {
        orders.updateStatus(changed);
        return changed;
    }

    private Order load(UUID orderId) {
        // Commands are only sent after the order commits, so a reply for an unknown order can't be fixed by retrying.
        return orders.findById(orderId).orElseThrow(
                () -> new NonRetryableMessageException("Reply for unknown order " + orderId));
    }

    private static void ignore(Order order, String messageType) {
        log.warn("Ignoring {} for order {} in status {}", messageType, order.id(), order.status());
    }

    /** {@code expiresAt} is the saga deadline: payment-service declines the command once it has passed. */
    private static ProcessPayment processPayment(Order order) {
        return new ProcessPayment(order.id(), order.customerId(), order.totalAmount(), order.currency(), order.deadlineAt());
    }

    /** Postgres stores microseconds; truncating keeps a saved order equal to the one read back. */
    private Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MICROS);
    }
}
