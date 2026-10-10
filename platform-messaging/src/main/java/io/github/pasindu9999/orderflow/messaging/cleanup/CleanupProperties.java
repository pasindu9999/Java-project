package io.github.pasindu9999.orderflow.messaging.cleanup;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Retention for the outbox and the inbox (ARCHITECTURE §7.1, §7.2).
 *
 * @param outboxRetention published outbox rows older than this are deleted; unpublished rows never are
 * @param inboxRetention  processed_message rows older than this are deleted. Longer than the outbox retention:
 *                        a producer can only re-send a message while its outbox row exists, so every possible
 *                        redelivery still finds its inbox row. A DLT replay after that is caught by the natural
 *                        keys (one reservation, one payment per order, the saga state machine).
 * @param interval        pause between cleanup runs
 * @param batchSize       rows per DELETE; each batch commits on its own, so no long transaction holds locks
 * @param enabled         false turns the background job off
 */
@ConfigurationProperties("orderflow.cleanup")
public record CleanupProperties(
        @DefaultValue("P7D") Duration outboxRetention,
        @DefaultValue("P14D") Duration inboxRetention,
        @DefaultValue("PT1H") Duration interval,
        @DefaultValue("1000") int batchSize,
        @DefaultValue("true") boolean enabled) {
}
