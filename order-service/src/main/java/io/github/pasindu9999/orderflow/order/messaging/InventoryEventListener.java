package io.github.pasindu9999.orderflow.order.messaging;

import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.messaging.inbox.IdempotentMessageHandler;
import io.github.pasindu9999.orderflow.order.app.OrderSaga;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Thin adapter: Kafka record in, idempotent handling, saga rules in {@link OrderSaga}. */
@Component
class InventoryEventListener {

    static final String CONSUMER = "inventory-events-handler";

    private final IdempotentMessageHandler inbox;
    private final OrderSaga saga;

    InventoryEventListener(IdempotentMessageHandler inbox, OrderSaga saga) {
        this.inbox = inbox;
        this.saga = saga;
    }

    @KafkaListener(topics = Topics.INVENTORY_EVENTS)
    void onEvent(String record) {
        inbox.handle(CONSUMER, record, saga::onInventoryEvent);
    }
}
