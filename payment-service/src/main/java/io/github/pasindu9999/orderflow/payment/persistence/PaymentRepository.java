package io.github.pasindu9999.orderflow.payment.persistence;

import io.github.pasindu9999.orderflow.payment.domain.DeclineReason;
import io.github.pasindu9999.orderflow.payment.domain.Payment;
import io.github.pasindu9999.orderflow.payment.domain.PaymentStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentRepository {

    private static final String SELECT = """
            SELECT id, order_id, customer_id, amount, currency, status, failure_reason FROM payment
            WHERE order_id = :orderId
            """;

    private final JdbcClient jdbc;

    public PaymentRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<Payment> findByOrderId(UUID orderId) {
        return jdbc.sql(SELECT).param("orderId", orderId).query(PaymentRepository::map).optional();
    }

    /** Like {@link #findByOrderId}, but locks the row until the transaction ends. */
    public Optional<Payment> lockByOrderId(UUID orderId) {
        return jdbc.sql(SELECT + " FOR UPDATE").param("orderId", orderId).query(PaymentRepository::map).optional();
    }

    /** Throws {@code DuplicateKeyException} if the order already has a payment row. */
    public void insert(Payment payment) {
        jdbc.sql("""
                        INSERT INTO payment (id, order_id, customer_id, amount, currency, status, failure_reason)
                        VALUES (:id, :orderId, :customerId, :amount, :currency, :status, :failureReason)
                        """)
                .param("id", payment.id())
                .param("orderId", payment.orderId())
                .param("customerId", payment.customerId())
                .param("amount", payment.amount())
                .param("currency", payment.currency())
                .param("status", payment.status().name())
                .param("failureReason", payment.failureReason() == null ? null : payment.failureReason().name(), Types.VARCHAR)
                .update();
    }

    public void updateStatus(UUID orderId, PaymentStatus status) {
        jdbc.sql("UPDATE payment SET status = :status, updated_at = now() WHERE order_id = :orderId")
                .param("orderId", orderId)
                .param("status", status.name())
                .update();
    }

    private static Payment map(ResultSet rs, int rowNum) throws SQLException {
        String failureReason = rs.getString("failure_reason");
        return new Payment(
                rs.getObject("id", UUID.class),
                rs.getObject("order_id", UUID.class),
                rs.getObject("customer_id", UUID.class),
                rs.getBigDecimal("amount"),
                rs.getString("currency"),
                PaymentStatus.valueOf(rs.getString("status")),
                failureReason == null ? null : DeclineReason.valueOf(failureReason));
    }
}
