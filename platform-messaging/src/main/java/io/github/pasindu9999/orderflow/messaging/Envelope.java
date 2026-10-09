package io.github.pasindu9999.orderflow.messaging;

import io.github.pasindu9999.orderflow.contracts.Message;
import java.time.Instant;
import java.util.UUID;

/**
 * Metadata wrapped around every message on the wire (ARCHITECTURE §5.1).
 *
 * @param messageId     created once, when the message is written to the outbox; the consumers' idempotency key
 * @param correlationId always the orderId, so one saga can be followed across services
 * @param causationId   messageId of the message that caused this one; {@code null} for the first message
 */
public record Envelope(
        UUID messageId,
        String messageType,
        int schemaVersion,
        Instant occurredAt,
        UUID correlationId,
        UUID causationId,
        String producer,
        Message payload) {

    /**
     * Narrows the payload to what a consumer of one topic expects, typically a sealed interface such as
     * {@code InventoryCommand}, so the handler can use an exhaustive {@code switch}. A message type that
     * doesn't belong on this topic can never be processed, so it is a {@link MessageParseException}.
     */
    public <T extends Message> T payloadAs(Class<T> expected) {
        if (!expected.isInstance(payload)) {
            throw new MessageParseException(
                    "Expected %s but got %s v%d".formatted(expected.getSimpleName(), messageType, schemaVersion));
        }
        return expected.cast(payload);
    }
}
