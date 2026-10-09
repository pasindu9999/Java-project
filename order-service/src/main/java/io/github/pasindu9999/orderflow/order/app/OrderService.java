package io.github.pasindu9999.orderflow.order.app;

import io.github.pasindu9999.orderflow.order.config.SagaProperties;
import io.github.pasindu9999.orderflow.order.domain.Order;
import io.github.pasindu9999.orderflow.order.domain.OrderDraft;
import io.github.pasindu9999.orderflow.order.persistence.OrderRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final SagaProperties saga;

    public OrderService(OrderRepository orders, TransactionTemplate transaction, Clock clock, SagaProperties saga) {
        this.orders = orders;
        this.transaction = transaction;
        this.clock = clock;
        this.saga = saga;
    }

    /**
     * Places an order exactly once per (customer, idempotency key).
     *
     * <ul>
     *   <li>Same key, same request: returns the existing order (a safe client retry).</li>
     *   <li>Same key, different request: {@link IdempotencyKeyReusedException}.</li>
     * </ul>
     *
     * <p>The pre-check handles ordinary retries. The unique index handles two requests racing on the same key:
     * the loser's insert fails, and it then reads the winner's committed order. The insert runs in its own
     * transaction because Postgres aborts a transaction after a constraint violation, so the read must happen
     * outside it.
     */
    public PlaceOrderResult placeOrder(String idempotencyKey, OrderDraft draft) {
        Order.requireValidIdempotencyKey(idempotencyKey);
        String requestHash = draft.fingerprint();

        var existing = orders.findByIdempotencyKey(draft.customerId(), idempotencyKey);
        if (existing.isPresent()) {
            return replay(existing.get(), requestHash);
        }

        Order order = Order.place(UUID.randomUUID(), draft, idempotencyKey, now(), saga.timeout());
        try {
            transaction.executeWithoutResult(status -> orders.insert(order));
        } catch (DuplicateKeyException raceLost) {
            Order winner = orders.findByIdempotencyKey(draft.customerId(), idempotencyKey).orElseThrow(() -> raceLost);
            return replay(winner, requestHash);
        }
        log.info("Order {} placed: {} line(s), total {} {}", order.id(), order.lines().size(), order.totalAmount(), order.currency());
        return new PlaceOrderResult(order, false);
    }

    public Order getOrder(UUID orderId) {
        return orders.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    private PlaceOrderResult replay(Order existing, String requestHash) {
        if (!existing.requestHash().equals(requestHash)) {
            throw new IdempotencyKeyReusedException(existing.idempotencyKey());
        }
        log.info("Order {} replayed for a retried request", existing.id());
        return new PlaceOrderResult(existing, true);
    }

    /** Postgres stores microseconds; truncating keeps a saved order equal to the one read back. */
    private Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MICROS);
    }
}
