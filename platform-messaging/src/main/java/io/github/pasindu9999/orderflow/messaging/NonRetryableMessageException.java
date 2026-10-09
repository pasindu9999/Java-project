package io.github.pasindu9999.orderflow.messaging;

/**
 * A well-formed message whose content can never be processed, e.g. a command with no lines. Retrying can't help,
 * so it goes straight to the dead-letter topic (ADR-0004), just like a {@link MessageParseException}.
 */
public class NonRetryableMessageException extends RuntimeException {

    public NonRetryableMessageException(String message) {
        super(message);
    }
}
