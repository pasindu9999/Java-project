package io.github.pasindu9999.orderflow.order.app;

import io.github.pasindu9999.orderflow.contracts.inventory.ReleaseInventory;
import io.github.pasindu9999.orderflow.messaging.FaultInjector;
import io.github.pasindu9999.orderflow.messaging.outbox.OutboxWriter;
import io.github.pasindu9999.orderflow.order.config.SagaProperties;
import io.github.pasindu9999.orderflow.order.domain.CancelReason;
import io.github.pasindu9999.orderflow.order.domain.Order;
import io.github.pasindu9999.orderflow.order.persistence.OrderRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Brings every saga to a terminal state, even when a reply never comes: the other service is down, or the
 * message sits in a DLT (ARCHITECTURE §7.4). An open order past its deadline is cancelled with {@code TIMEOUT},
 * and {@code ReleaseInventory} is written in the same transaction. If inventory never reserved anything, that
 * release leaves a tombstone, so a late reservation is refused.
 *
 * <p>The sweeper and the reply handlers can act on the same order at the same time. Both use the optimistic
 * {@code version} update, so exactly one wins. When the sweeper loses, it skips the order; the next run
 * re-evaluates it from its new state.
 */
@Service
public class OrderTimeoutSweeper {

    /** Between reading an expired order and cancelling it: where a reply can win the race. */
    public static final String BEFORE_CANCEL = "timeout-sweeper.before-cancel";

    private static final Logger log = LoggerFactory.getLogger(OrderTimeoutSweeper.class);

    private final OrderRepository orders;
    private final OutboxWriter outbox;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final SagaProperties saga;
    private final FaultInjector faults;

    public OrderTimeoutSweeper(OrderRepository orders, OutboxWriter outbox, TransactionTemplate transaction, Clock clock,
                               SagaProperties saga, FaultInjector faults) {
        this.orders = orders;
        this.outbox = outbox;
        this.transaction = transaction;
        this.clock = clock;
        this.saga = saga;
        this.faults = faults;
    }

    /**
     * Cancels up to {@code orderflow.saga.sweep-batch-size} expired orders, each in its own transaction, so one
     * lost race doesn't undo the others.
     *
     * @return how many orders this run cancelled
     */
    public int sweep() {
        Instant now = clock.instant().truncatedTo(ChronoUnit.MICROS);
        int cancelled = 0;
        for (UUID orderId : orders.findExpiredOpenOrderIds(now, saga.sweepBatchSize())) {
            if (cancelIfStillExpired(orderId, now)) {
                cancelled++;
            }
        }
        return cancelled;
    }

    private boolean cancelIfStillExpired(UUID orderId, Instant now) {
        try (var correlation = MDC.putCloseable("correlationId", orderId.toString())) {
            return Boolean.TRUE.equals(transaction.execute(status -> {
                Order order = orders.findById(orderId).orElseThrow();
                if (order.status().isTerminal() || !order.deadlineAt().isBefore(now)) {
                    return false; // a reply finished it after the query
                }
                faults.at(BEFORE_CANCEL);
                orders.updateStatus(order.cancel(CancelReason.TIMEOUT, now));
                outbox.write(new ReleaseInventory(orderId, ReleaseInventory.Reason.TIMEOUT), null);
                log.info("Order {} timed out in {}; cancelled, releasing its stock", orderId, order.status());
                return true;
            }));
        } catch (OptimisticLockingFailureException replyWon) {
            log.info("Order {} changed while timing out; a reply won the race, the next sweep re-checks it", orderId);
            return false;
        }
    }
}
