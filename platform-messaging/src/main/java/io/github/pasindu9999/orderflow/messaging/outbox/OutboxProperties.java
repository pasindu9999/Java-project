package io.github.pasindu9999.orderflow.messaging.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param pollInterval pause between relay runs when the outbox is drained (ADR-0002)
 * @param batchSize    rows read per relay transaction
 * @param sendTimeout  how long one synchronous Kafka send may take before the batch stops
 * @param relayEnabled false turns off the background relay, e.g. for tests that call it directly
 */
@ConfigurationProperties("orderflow.outbox")
public record OutboxProperties(
        @DefaultValue("200ms") Duration pollInterval,
        @DefaultValue("100") int batchSize,
        @DefaultValue("5s") Duration sendTimeout,
        @DefaultValue("true") boolean relayEnabled) {
}
