package io.github.pasindu9999.orderflow.messaging.inbox;

import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Processes each message at most once per consumer, even though Kafka delivers it at least once (ADR-0003).
 *
 * <p>One transaction per message:
 * <ol>
 *   <li>{@code INSERT INTO processed_message ... ON CONFLICT DO NOTHING}. Zero rows means this consumer already
 *       processed the message, so it is skipped.</li>
 *   <li>Run the business handler: state changes plus outbox rows for any replies.</li>
 *   <li>Commit. Only then does the listener container commit the Kafka offset.</li>
 * </ol>
 * The inbox row is inserted <em>first</em> on purpose: if two consumers race on the same message (e.g. during a
 * rebalance), the second insert blocks on the primary key until the first commits, then sees the conflict.
 * Checking first and inserting last would let both pass the check.
 */
public class IdempotentMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(IdempotentMessageHandler.class);

    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;
    private final MessageCodec codec;
    private final MeterRegistry meters;

    public IdempotentMessageHandler(JdbcClient jdbc, TransactionTemplate transaction, MessageCodec codec, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.codec = codec;
        this.meters = meters;
    }

    /**
     * @param consumer name of the handling consumer, e.g. {@code inventory-commands-handler}; the same message may
     *                 be processed once by each consumer
     * @param record   the raw Kafka record value
     * @return true if the handler ran, false if the message was a duplicate and was skipped
     * @throws io.github.pasindu9999.orderflow.messaging.MessageParseException if the record isn't a valid envelope
     */
    public boolean handle(String consumer, String record, Consumer<Envelope> handler) {
        Envelope envelope = codec.decode(record);
        try (var correlation = MDC.putCloseable("correlationId", envelope.correlationId().toString());
             var message = MDC.putCloseable("messageId", envelope.messageId().toString())) {
            Boolean processed = transaction.execute(status -> {
                int inserted = jdbc.sql("""
                                INSERT INTO processed_message (consumer, message_id) VALUES (:consumer, :messageId)
                                ON CONFLICT DO NOTHING
                                """)
                        .param("consumer", consumer)
                        .param("messageId", envelope.messageId())
                        .update();
                if (inserted == 0) {
                    return false;
                }
                handler.accept(envelope);
                return true;
            });
            if (!Boolean.TRUE.equals(processed)) {
                log.info("Skipped duplicate {} {} for {}", envelope.messageType(), envelope.messageId(), consumer);
                duplicates(consumer).increment();
                return false;
            }
            return true;
        }
    }

    private Counter duplicates(String consumer) {
        return Counter.builder("messaging.duplicates.skipped")
                .description("Messages skipped because this consumer had already processed them")
                .tag("consumer", consumer)
                .register(meters);
    }
}
