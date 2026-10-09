package io.github.pasindu9999.orderflow.payment.domain;

import java.math.BigDecimal;
import java.util.Set;
import java.util.UUID;

/**
 * The deterministic payment simulator (ARCHITECTURE §6.4). Pure function: the caller counts attempts and turns
 * the decision into rows, replies or a retryable exception.
 *
 * <p>Command expiry (§6.4 rule 1) is added on Day 10 together with the saga timeout.
 */
public final class PaymentPolicy {

    /**
     * @param transientFailureAmount the provider "fails" for this exact amount ...
     * @param transientFailures      ... on this many attempts per order, then succeeds (demos retries)
     */
    public record Rules(Set<UUID> blockedCustomers, BigDecimal declineAbove,
                        BigDecimal transientFailureAmount, int transientFailures) {

        public Rules {
            blockedCustomers = Set.copyOf(blockedCustomers);
        }
    }

    public sealed interface Decision permits Approve, Decline, ProviderUnavailable {
    }

    public record Approve() implements Decision {
    }

    public record Decline(DeclineReason reason) implements Decision {
    }

    /** A simulated provider outage: retrying may succeed. */
    public record ProviderUnavailable() implements Decision {
    }

    private PaymentPolicy() {
    }

    /**
     * Rules are checked in order: blocked customer, then the amount limit, then the flaky amount.
     *
     * @param attempt 1 for the first evaluation of this order's charge, 2 for the first retry, and so on
     */
    public static Decision decide(UUID customerId, BigDecimal amount, int attempt, Rules rules) {
        if (rules.blockedCustomers().contains(customerId)) {
            return new Decline(DeclineReason.DECLINED_BLOCKED_CUSTOMER);
        }
        if (amount.compareTo(rules.declineAbove()) > 0) {
            return new Decline(DeclineReason.DECLINED_LIMIT);
        }
        if (amount.compareTo(rules.transientFailureAmount()) == 0 && attempt <= rules.transientFailures()) {
            return new ProviderUnavailable();
        }
        return new Approve();
    }
}
