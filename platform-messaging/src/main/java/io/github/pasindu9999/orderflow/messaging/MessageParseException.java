package io.github.pasindu9999.orderflow.messaging;

/**
 * A record that can never be processed: malformed JSON, a missing envelope field, or an unknown message type
 * or schema version. Non-retryable, so it goes straight to the dead-letter topic (ADR-0004).
 */
public class MessageParseException extends RuntimeException {

    public MessageParseException(String message) {
        super(message);
    }

    public MessageParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
