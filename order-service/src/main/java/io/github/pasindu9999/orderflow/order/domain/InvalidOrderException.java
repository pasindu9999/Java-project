package io.github.pasindu9999.orderflow.order.domain;

import java.util.List;

/** The order request breaks one or more business rules. Carries every violation, not just the first. */
public class InvalidOrderException extends RuntimeException {

    private final List<String> violations;

    public InvalidOrderException(List<String> violations) {
        super("Invalid order: " + String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}
