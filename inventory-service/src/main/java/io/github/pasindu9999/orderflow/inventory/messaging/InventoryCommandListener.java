package io.github.pasindu9999.orderflow.inventory.messaging;

import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.inventory.app.ReservationService;
import io.github.pasindu9999.orderflow.messaging.inbox.IdempotentMessageHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Thin adapter: Kafka record in, idempotent handling, business logic in {@link ReservationService}. */
@Component
class InventoryCommandListener {

    static final String CONSUMER = "inventory-commands-handler";

    private final IdempotentMessageHandler inbox;
    private final ReservationService reservations;

    InventoryCommandListener(IdempotentMessageHandler inbox, ReservationService reservations) {
        this.inbox = inbox;
        this.reservations = reservations;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
    void onCommand(String record) {
        inbox.handle(CONSUMER, record, reservations::handle);
    }
}
