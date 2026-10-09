package io.github.pasindu9999.orderflow.contracts.inventory;

import io.github.pasindu9999.orderflow.contracts.Message;

/** Events published by inventory-service on {@code inventory.events}. Owned by inventory-service. */
public sealed interface InventoryEvent extends Message permits InventoryReserved, InventoryRejected {
}
