package io.github.pasindu9999.orderflow.contracts.inventory;

import io.github.pasindu9999.orderflow.contracts.Message;

/** Commands handled by inventory-service, published on {@code inventory.commands}. Owned by inventory-service. */
public sealed interface InventoryCommand extends Message permits ReserveInventory, ReleaseInventory {
}
