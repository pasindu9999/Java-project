package io.github.pasindu9999.orderflow.contracts.inventory;

import java.util.List;
import java.util.UUID;

/** Nothing was reserved. {@code shortages} lists the lines that could not be satisfied. */
public record InventoryRejected(UUID orderId, Reason reason, List<Shortage> shortages) implements InventoryEvent {

    public enum Reason {
        OUT_OF_STOCK,
        UNKNOWN_SKU,
        ALREADY_RELEASED
    }

    public record Shortage(String sku, int requested, int available) {
    }
}
