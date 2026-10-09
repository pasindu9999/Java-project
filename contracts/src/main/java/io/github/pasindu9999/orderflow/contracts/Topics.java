package io.github.pasindu9999.orderflow.contracts;

/** Kafka topic names (ARCHITECTURE §4). Every record is keyed by orderId. */
public final class Topics {

    public static final String INVENTORY_COMMANDS = "inventory.commands";
    public static final String INVENTORY_EVENTS = "inventory.events";
    public static final String PAYMENT_COMMANDS = "payment.commands";
    public static final String PAYMENT_EVENTS = "payment.events";

    private Topics() {
    }
}
