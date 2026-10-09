package io.github.pasindu9999.orderflow.contracts.inventory;

import java.util.UUID;

/** Stock for every line of the order is reserved. */
public record InventoryReserved(UUID orderId) implements InventoryEvent {
}
