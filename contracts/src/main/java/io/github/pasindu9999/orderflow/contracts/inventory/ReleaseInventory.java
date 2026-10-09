package io.github.pasindu9999.orderflow.contracts.inventory;

import java.util.UUID;

/** Compensation: return the order's reserved stock. Idempotent, and never fails for business reasons. */
public record ReleaseInventory(UUID orderId, Reason reason) implements InventoryCommand {

    public enum Reason {
        PAYMENT_DECLINED,
        TIMEOUT,
        LATE_RESERVATION
    }
}
