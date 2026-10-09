package io.github.pasindu9999.orderflow.messaging.error;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Blocking retry budget for a failing record (ADR-0004). The defaults give 500 ms + 1 s + 2 s ≈ 3.5 s, far below
 * {@code max.poll.interval.ms}, so a retrying consumer is never kicked out of its group.
 *
 * @param maxRetries retries after the first attempt; then the record goes to the DLT
 */
@ConfigurationProperties("orderflow.consumer.retry")
public record ConsumerRetryProperties(
        @DefaultValue("500ms") Duration initialInterval,
        @DefaultValue("2.0") double multiplier,
        @DefaultValue("3") int maxRetries) {
}
