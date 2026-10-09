package io.github.pasindu9999.orderflow.inventory.domain;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** All-or-nothing reservation rules (ARCHITECTURE §6.3). Pure functions: the caller does the locking and writing. */
public final class ReservationPolicy {

    public sealed interface Decision permits Accept, Reject {
    }

    /** Reserve exactly these lines. */
    public record Accept(List<ReservationLine> lines) implements Decision {
    }

    /** Reserve nothing. */
    public record Reject(RejectionReason reason, List<Shortage> shortages) implements Decision {
    }

    private ReservationPolicy() {
    }

    /**
     * Merges lines for the same SKU and sorts them by SKU. The sort order is also the order the caller locks stock
     * rows in: two orders that share SKUs then always lock them in the same sequence, so they can't deadlock.
     *
     * @throws IllegalArgumentException if the request has no lines, a blank SKU or a non-positive quantity
     */
    public static List<ReservationLine> normalise(List<ReservationLine> lines) {
        if (lines == null || lines.isEmpty()) {
            throw new IllegalArgumentException("A reservation needs at least one line");
        }
        Map<String, Integer> merged = new TreeMap<>();
        for (ReservationLine line : lines) {
            if (line == null || line.sku() == null || line.sku().isBlank()) {
                throw new IllegalArgumentException("Every line needs a SKU");
            }
            if (line.quantity() <= 0) {
                throw new IllegalArgumentException("Quantity must be positive for " + line.sku());
            }
            merged.merge(line.sku(), line.quantity(), Math::addExact);
        }
        return merged.entrySet().stream().map(e -> new ReservationLine(e.getKey(), e.getValue())).toList();
    }

    /**
     * @param lines normalised lines (see {@link #normalise})
     * @param stock current, locked stock for those SKUs; an absent SKU is unknown
     */
    public static Decision decide(List<ReservationLine> lines, Map<String, StockLevel> stock) {
        List<Shortage> unknown = lines.stream()
                .filter(line -> !stock.containsKey(line.sku()))
                .map(line -> new Shortage(line.sku(), line.quantity(), 0))
                .toList();
        if (!unknown.isEmpty()) {
            return new Reject(RejectionReason.UNKNOWN_SKU, unknown);
        }
        List<Shortage> shortages = lines.stream()
                .filter(line -> stock.get(line.sku()).available() < line.quantity())
                .map(line -> new Shortage(line.sku(), line.quantity(), stock.get(line.sku()).available()))
                .toList();
        if (!shortages.isEmpty()) {
            return new Reject(RejectionReason.OUT_OF_STOCK, shortages);
        }
        return new Accept(lines);
    }
}
