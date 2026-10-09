package io.github.pasindu9999.orderflow.messaging.outbox;

import io.github.pasindu9999.orderflow.contracts.Message;
import io.github.pasindu9999.orderflow.messaging.Envelope;
import io.github.pasindu9999.orderflow.messaging.FaultInjector;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Writes an outgoing message into the {@code outbox} table, inside the caller's transaction (ADR-0002).
 *
 * <p>This is the only way business code sends a message. Because the row commits or rolls back together with
 * the business change, a state change can never happen without its message, and vice versa.
 */
public class OutboxWriter {

    /** Fault point: the row is inserted but the surrounding transaction hasn't committed yet. */
    public static final String AFTER_WRITE = "outbox-writer.after-write";

    private final JdbcClient jdbc;
    private final MessageCodec codec;
    private final FaultInjector faults;

    public OutboxWriter(JdbcClient jdbc, MessageCodec codec, FaultInjector faults) {
        this.jdbc = jdbc;
        this.codec = codec;
        this.faults = faults;
    }

    /**
     * @param causationId messageId of the message being handled, or {@code null} when the message starts a saga
     * @return the envelope exactly as it will be relayed, including its new messageId
     * @throws IllegalStateException when called outside a transaction, where the atomicity guarantee would be lost
     */
    public Envelope write(Message payload, UUID causationId) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("OutboxWriter must run inside the business transaction");
        }
        Envelope envelope = codec.wrap(payload, causationId);
        jdbc.sql("""
                        INSERT INTO outbox (message_id, topic, message_key, message_type, envelope)
                        VALUES (:messageId, :topic, :messageKey, :messageType, CAST(:envelope AS jsonb))
                        """)
                .param("messageId", envelope.messageId())
                .param("topic", codec.topicFor(payload))
                .param("messageKey", envelope.correlationId().toString())
                .param("messageType", envelope.messageType())
                .param("envelope", codec.encode(envelope))
                .update();
        faults.at(AFTER_WRITE);
        return envelope;
    }
}
