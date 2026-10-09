package io.github.pasindu9999.orderflow.contracts.inventory;

import java.util.List;
import java.util.UUID;

/** Reserve stock for every line of the order, or for none of them. */
public record ReserveInventory(UUID orderId, List<Line> lines) implements InventoryCommand {

    public record Line(String sku, int quantity) {
    }
}
