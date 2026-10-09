package io.github.pasindu9999.orderflow.order.app;

import io.github.pasindu9999.orderflow.contracts.inventory.InventoryEvent;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryRejected;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryReserved;
import io.github.pasindu9999.orderflow.contracts.inventory.ReleaseInventory;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentEvent;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentFailed;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentRefunded;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentSucceeded;
import io.github.pasindu9999.orderflow.contracts.payment.ProcessPayment;
import io.github.pasindu9999.orderflow.contracts.payment.RefundPayment;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.FaultInjector;
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
 * <p>Every status change is an optimistic update. If another writer (the timeout sweeper) changed the order in
 * between, the update throws a retryable exception and the redelivered reply is evaluated against the new state.
 */
@Service
public class OrderSaga {

    /** Between reading the order and saving its new status: where a concurrent writer can slip in. */
    public static final String BEFORE_STATUS_UPDATE = "order-saga.before-status-update";

    private static final Logger log = LoggerFactory.getLogger(OrderSaga.class);

    private final OrderRepository orders;
    private final OutboxWriter outbox;
    private final Clock clock;
    private final FaultInjector faults;

    public OrderSaga(OrderRepository orders, OutboxWriter outbox, Clock clock, FaultInjector faults) {
        this.orders = orders;
        this.outbox = outbox;
        this.clock = clock;
        this.faults = faults;
    }

    /** MANDATORY: only ever called from the idempotent inbox handler, which owns the transaction. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void onInventoryEvent(Envelope envelope) {
        switch (envelope.payloadAs(InventoryEvent.class)) {
            case InventoryReserved event -> onReserved(load(event.orderId()), envelope.messageId());
            case InventoryRejected event -> onRejected(load(event.orderId()), event);
        }
    }

    /** MANDATORY: only ever called from the idempotent inbox handler, which owns the transaction. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void onPaymentEvent(Envelope envelope) {
        switch (envelope.payloadAs(PaymentEvent.class)) {
            case PaymentSucceeded event -> onPaymentSucceeded(load(event.orderId()), envelope.messageId());
            case PaymentFailed event -> onPaymentFailed(load(event.orderId()), event, envelope.messageId());
            case PaymentRefunded event -> onPaymentRefunded(load(event.orderId()));
        }
    }

    private void onReserved(Order order, UUID causationId) {
        switch (order.status()) {
            case PENDING -> {
                Order awaiting = save(order.awaitPayment(now()));
                outbox.write(processPayment(awaiting), causationId);
                log.info("Order {} reserved; awaiting payment of {} {}", order.id(), order.totalAmount(), order.currency());
            }
            case CANCELLED -> {
                // Late reply: the order was cancelled (e.g. timed out) before inventory answered. Give the stock back.
                outbox.write(new ReleaseInventory(order.id(), ReleaseInventory.Reason.LATE_RESERVATION), causationId);
                log.info("Order {} is already cancelled ({}); releasing its late reservation", order.id(), order.cancelReason());
            }
            case AWAITING_PAYMENT, CONFIRMED -> ignore(order, "InventoryReserved");
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

    private void onPaymentSucceeded(Order order, UUID causationId) {
        switch (order.status()) {
            case AWAITING_PAYMENT -> {
                save(order.confirm(now()));
                log.info("Order {} confirmed", order.id());
            }
            case CANCELLED -> {
                // Late reply: the charge landed after the order was cancelled (e.g. timed out). Payment is the
                // pivot, so the only way back is a refund (ARCHITECTURE §8.7). A refund with nothing to refund
                // is a no-op on payment's side, so this never needs to check why the order was cancelled.
                outbox.write(new RefundPayment(order.id(), RefundPayment.Reason.LATE_PAYMENT), causationId);
                log.info("Order {} is already cancelled ({}); refunding its late payment", order.id(), order.cancelReason());
            }
            case PENDING, CONFIRMED -> ignore(order, "PaymentSucceeded");
        }
    }

    private void onPaymentFailed(Order order, PaymentFailed event, UUID causationId) {
        switch (order.status()) {
            case AWAITING_PAYMENT -> {
                // Compensate the step before the pivot: the stock was reserved for this order.
                save(order.cancel(CancelReason.PAYMENT_DECLINED, now()));
                outbox.write(new ReleaseInventory(order.id(), ReleaseInventory.Reason.PAYMENT_DECLINED), causationId);
                log.info("Order {} cancelled: payment failed ({}); releasing its stock", order.id(), event.reason());
            }
            case CANCELLED -> log.info("Order {} is already cancelled; recorded late PaymentFailed", order.id());
            case PENDING, CONFIRMED -> ignore(order, "PaymentFailed");
        }
    }

    private void onPaymentRefunded(Order order) {
        switch (order.status()) {
            case CANCELLED -> log.info("Order {} is already cancelled; recorded PaymentRefunded", order.id());
            case PENDING, AWAITING_PAYMENT, CONFIRMED -> ignore(order, "PaymentRefunded");
        }
    }

    private Order save(Order changed) {
        faults.at(BEFORE_STATUS_UPDATE);
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
