package io.github.pasindu9999.orderflow.order.messaging;

import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.messaging.inbox.IdempotentMessageHandler;
import io.github.pasindu9999.orderflow.order.app.OrderSaga;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Thin adapter: Kafka record in, idempotent handling, saga rules in {@link OrderSaga}. */
@Component
class PaymentEventListener {

    static final String CONSUMER = "payment-events-handler";

    private final IdempotentMessageHandler inbox;
    private final OrderSaga saga;

    PaymentEventListener(IdempotentMessageHandler inbox, OrderSaga saga) {
        this.inbox = inbox;
        this.saga = saga;
    }

    @KafkaListener(topics = Topics.PAYMENT_EVENTS)
    void onEvent(String record) {
        inbox.handle(CONSUMER, record, saga::onPaymentEvent);
    }
}
