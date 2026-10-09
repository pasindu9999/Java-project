package io.github.pasindu9999.orderflow.contracts;

import io.github.pasindu9999.orderflow.contracts.inventory.InventoryRejected;
import io.github.pasindu9999.orderflow.contracts.inventory.InventoryReserved;
import io.github.pasindu9999.orderflow.contracts.inventory.ReleaseInventory;
import io.github.pasindu9999.orderflow.contracts.inventory.ReserveInventory;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentFailed;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentRefunded;
import io.github.pasindu9999.orderflow.contracts.payment.PaymentSucceeded;
import io.github.pasindu9999.orderflow.contracts.payment.ProcessPayment;
import io.github.pasindu9999.orderflow.contracts.payment.RefundPayment;
import java.util.List;

/**
 * Every message type on the wire: its name, schema version, payload record and topic (ADR-0005).
 *
 * <p>Type names are explicit strings, never derived from class names, so renaming a Java class can't
 * silently change the wire format. A breaking change adds a new entry with the next version; it never
 * edits an existing one.
 */
public final class MessageCatalog {

    public record Entry(String type, int version, Class<? extends Message> payloadClass, String topic) {
    }

    public static final List<Entry> ENTRIES = List.of(
            new Entry("ReserveInventory", 1, ReserveInventory.class, Topics.INVENTORY_COMMANDS),
            new Entry("ReleaseInventory", 1, ReleaseInventory.class, Topics.INVENTORY_COMMANDS),
            new Entry("InventoryReserved", 1, InventoryReserved.class, Topics.INVENTORY_EVENTS),
            new Entry("InventoryRejected", 1, InventoryRejected.class, Topics.INVENTORY_EVENTS),
            new Entry("ProcessPayment", 1, ProcessPayment.class, Topics.PAYMENT_COMMANDS),
            new Entry("RefundPayment", 1, RefundPayment.class, Topics.PAYMENT_COMMANDS),
            new Entry("PaymentSucceeded", 1, PaymentSucceeded.class, Topics.PAYMENT_EVENTS),
            new Entry("PaymentFailed", 1, PaymentFailed.class, Topics.PAYMENT_EVENTS),
            new Entry("PaymentRefunded", 1, PaymentRefunded.class, Topics.PAYMENT_EVENTS));

    private MessageCatalog() {
    }
}
