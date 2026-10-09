package io.github.pasindu9999.orderflow.inventory.persistence;

import io.github.pasindu9999.orderflow.inventory.domain.StockLevel;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class StockRepository {

    private final JdbcClient jdbc;

    public StockRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Locks the stock rows for these SKUs until the transaction ends, in SKU order. Postgres takes the row locks
     * in the order the sorted rows come out, so every order locks shared SKUs in the same sequence and two
     * reservations can never deadlock. Unknown SKUs are simply absent from the result.
     */
    public Map<String, StockLevel> lockForUpdate(Collection<String> skus) {
        Map<String, StockLevel> locked = new LinkedHashMap<>();
        jdbc.sql("SELECT sku, available, reserved FROM product_stock WHERE sku IN (:skus) ORDER BY sku FOR UPDATE")
                .param("skus", skus)
                .query(StockRepository::map)
                .list()
                .forEach(level -> locked.put(level.sku(), level));
        return locked;
    }

    /** Moves units from available to reserved. Must run under {@link #lockForUpdate}, after the policy approved it. */
    public void reserve(String sku, int quantity) {
        int updated = jdbc.sql("""
                        UPDATE product_stock
                        SET available = available - :quantity, reserved = reserved + :quantity, updated_at = now()
                        WHERE sku = :sku AND available >= :quantity
                        """)
                .param("sku", sku)
                .param("quantity", quantity)
                .update();
        if (updated != 1) {
            // Can't happen while the row is locked and checked; failing loudly beats overselling.
            throw new IllegalStateException("Stock for " + sku + " changed under its lock");
        }
    }

    public Optional<StockLevel> find(String sku) {
        return jdbc.sql("SELECT sku, available, reserved FROM product_stock WHERE sku = :sku")
                .param("sku", sku)
                .query(StockRepository::map)
                .optional();
    }

    private static StockLevel map(ResultSet rs, int rowNum) throws SQLException {
        return new StockLevel(rs.getString("sku"), rs.getInt("available"), rs.getInt("reserved"));
    }
}
