package io.github.pasindu9999.orderflow.payment.messaging;

import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.messaging.inbox.IdempotentMessageHandler;
import io.github.pasindu9999.orderflow.payment.app.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Thin adapter: Kafka record in, idempotent handling, business logic in {@link PaymentService}. */
@Component
class PaymentCommandListener {

    static final String CONSUMER = "payment-commands-handler";

    private final IdempotentMessageHandler inbox;
    private final PaymentService payments;

    PaymentCommandListener(IdempotentMessageHandler inbox, PaymentService payments) {
        this.inbox = inbox;
        this.payments = payments;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMMANDS)
    void onCommand(String record) {
        inbox.handle(CONSUMER, record, payments::handle);
    }
}
