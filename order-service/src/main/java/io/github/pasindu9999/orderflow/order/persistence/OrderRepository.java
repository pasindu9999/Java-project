package io.github.pasindu9999.orderflow.order.persistence;

import io.github.pasindu9999.orderflow.order.domain.CancelReason;
import io.github.pasindu9999.orderflow.order.domain.Order;
import io.github.pasindu9999.orderflow.order.domain.OrderLine;
import io.github.pasindu9999.orderflow.order.domain.OrderStatus;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Orders and their lines, with plain SQL (ADR-0007). Callers own the transaction. */
@Repository
public class OrderRepository {

    private static final String SELECT_ORDERS = """
            SELECT id, customer_id, status, cancel_reason, total_amount, currency, idempotency_key, request_hash,
                   deadline_at, created_at, updated_at, version
            FROM orders
            """;

    private final JdbcClient jdbc;

    public OrderRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Throws {@code DuplicateKeyException} when the customer already used this idempotency key. */
    public void insert(Order order) {
        jdbc.sql("""
                        INSERT INTO orders (id, customer_id, status, cancel_reason, total_amount, currency,
                                            idempotency_key, request_hash, deadline_at, created_at, updated_at, version)
                        VALUES (:id, :customerId, :status, :cancelReason, :totalAmount, :currency,
                                :idempotencyKey, :requestHash, :deadlineAt, :createdAt, :updatedAt, :version)
                        """)
                .param("id", order.id())
                .param("customerId", order.customerId())
                .param("status", order.status().name())
                .param("cancelReason", order.cancelReason() == null ? null : order.cancelReason().name(), Types.VARCHAR)
                .param("totalAmount", order.totalAmount())
                .param("currency", order.currency())
                .param("idempotencyKey", order.idempotencyKey())
                .param("requestHash", order.requestHash())
                .param("deadlineAt", utc(order.deadlineAt()))
                .param("createdAt", utc(order.createdAt()))
                .param("updatedAt", utc(order.updatedAt()))
                .param("version", order.version())
                .update();

        for (OrderLine line : order.lines()) {
            jdbc.sql("""
                            INSERT INTO order_lines (order_id, line_no, sku, quantity, unit_price)
                            VALUES (:orderId, :lineNo, :sku, :quantity, :unitPrice)
                            """)
                    .param("orderId", order.id())
                    .param("lineNo", line.lineNo())
                    .param("sku", line.sku())
                    .param("quantity", line.quantity())
                    .param("unitPrice", line.unitPrice())
                    .update();
        }
    }

    /**
     * Saves a status change. {@code order} still carries the version it was read with; the update only applies if
     * nobody changed the row since (ARCHITECTURE §3.3).
     *
     * @throws OptimisticLockingFailureException if the row changed in between, e.g. the timeout sweeper cancelled
     *         the order. It is retryable: the redelivered message is then evaluated against the new state.
     */
    public void updateStatus(Order order) {
        int updated = jdbc.sql("""
                        UPDATE orders
                        SET status = :status, cancel_reason = :cancelReason, updated_at = :updatedAt, version = version + 1
                        WHERE id = :id AND version = :version
                        """)
                .param("id", order.id())
                .param("status", order.status().name())
                .param("cancelReason", order.cancelReason() == null ? null : order.cancelReason().name(), Types.VARCHAR)
                .param("updatedAt", utc(order.updatedAt()))
                .param("version", order.version())
                .update();
        if (updated != 1) {
            throw new OptimisticLockingFailureException(
                    "Order %s changed since version %d".formatted(order.id(), order.version()));
        }
    }

    public Optional<Order> findById(UUID id) {
        return findOne("WHERE id = :id", Map.of("id", id));
    }

    public Optional<Order> findByIdempotencyKey(UUID customerId, String idempotencyKey) {
        return findOne("WHERE customer_id = :customerId AND idempotency_key = :key",
                Map.of("customerId", customerId, "key", idempotencyKey));
    }

    private Optional<Order> findOne(String where, Map<String, ?> params) {
        return jdbc.sql(SELECT_ORDERS + where)
                .params(params)
                .query(OrderRepository::mapHeader)
                .optional()
                .map(header -> header.withLines(findLines(header.id())));
    }

    private List<OrderLine> findLines(UUID orderId) {
        return jdbc.sql("""
                        SELECT line_no, sku, quantity, unit_price FROM order_lines
                        WHERE order_id = :orderId ORDER BY line_no
                        """)
                .param("orderId", orderId)
                .query((rs, rowNum) -> new OrderLine(
                        rs.getInt("line_no"), rs.getString("sku"), rs.getInt("quantity"), rs.getBigDecimal("unit_price")))
                .list();
    }

    /** The orders row without its lines, which come from a second query. */
    private record OrderHeader(UUID id, UUID customerId, OrderStatus status, CancelReason cancelReason,
                               BigDecimal totalAmount, String currency, String idempotencyKey, String requestHash,
                               Instant deadlineAt, Instant createdAt, Instant updatedAt, long version) {

        Order withLines(List<OrderLine> lines) {
            return new Order(id, customerId, status, cancelReason, totalAmount, currency, idempotencyKey, requestHash,
                    deadlineAt, createdAt, updatedAt, version, lines);
        }
    }

    private static OrderHeader mapHeader(ResultSet rs, int rowNum) throws SQLException {
        String cancelReason = rs.getString("cancel_reason");
        return new OrderHeader(
                rs.getObject("id", UUID.class),
                rs.getObject("customer_id", UUID.class),
                OrderStatus.valueOf(rs.getString("status")),
                cancelReason == null ? null : CancelReason.valueOf(cancelReason),
                rs.getBigDecimal("total_amount"),
                rs.getString("currency"),
                rs.getString("idempotency_key"),
                rs.getString("request_hash"),
                instant(rs, "deadline_at"),
                instant(rs, "created_at"),
                instant(rs, "updated_at"),
                rs.getLong("version"));
    }

    private static OffsetDateTime utc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, OffsetDateTime.class).toInstant();
    }
}
