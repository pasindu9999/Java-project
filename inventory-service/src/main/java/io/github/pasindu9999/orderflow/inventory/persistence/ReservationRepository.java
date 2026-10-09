package io.github.pasindu9999.orderflow.inventory.persistence;

import io.github.pasindu9999.orderflow.inventory.domain.ReservationLine;
import io.github.pasindu9999.orderflow.inventory.domain.ReservationStatus;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class ReservationRepository {

    private final JdbcClient jdbc;

    public ReservationRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<ReservationStatus> findStatus(UUID orderId) {
        return jdbc.sql("SELECT status FROM reservation WHERE order_id = :orderId")
                .param("orderId", orderId)
                .query((rs, rowNum) -> ReservationStatus.valueOf(rs.getString("status")))
                .optional();
    }

    /** Like {@link #findStatus}, but locks the row until the transaction ends. */
    public Optional<ReservationStatus> lockStatus(UUID orderId) {
        return jdbc.sql("SELECT status FROM reservation WHERE order_id = :orderId FOR UPDATE")
                .param("orderId", orderId)
                .query((rs, rowNum) -> ReservationStatus.valueOf(rs.getString("status")))
                .optional();
    }

    public void updateStatus(UUID orderId, ReservationStatus status) {
        jdbc.sql("UPDATE reservation SET status = :status, updated_at = now() WHERE order_id = :orderId")
                .param("orderId", orderId)
                .param("status", status.name())
                .update();
    }

    /** Throws {@code DuplicateKeyException} if the order already has a reservation row. */
    public void insert(UUID orderId, ReservationStatus status, List<ReservationLine> lines) {
        jdbc.sql("INSERT INTO reservation (order_id, status) VALUES (:orderId, :status)")
                .param("orderId", orderId)
                .param("status", status.name())
                .update();
        for (ReservationLine line : lines) {
            jdbc.sql("INSERT INTO reservation_line (order_id, sku, quantity) VALUES (:orderId, :sku, :quantity)")
                    .param("orderId", orderId)
                    .param("sku", line.sku())
                    .param("quantity", line.quantity())
                    .update();
        }
    }

    public List<ReservationLine> findLines(UUID orderId) {
        return jdbc.sql("SELECT sku, quantity FROM reservation_line WHERE order_id = :orderId ORDER BY sku")
                .param("orderId", orderId)
                .query((rs, rowNum) -> new ReservationLine(rs.getString("sku"), rs.getInt("quantity")))
                .list();
    }
}
