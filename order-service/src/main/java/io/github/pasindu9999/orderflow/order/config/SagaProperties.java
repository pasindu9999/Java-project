package io.github.pasindu9999.orderflow.order.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param timeout        how long an order's saga may run before it is cancelled (ARCHITECTURE §7.4)
 * @param sweepInterval  pause between two runs of the timeout sweeper
 * @param sweepBatchSize at most this many expired orders are cancelled per run; the rest wait for the next one
 */
@ConfigurationProperties("orderflow.saga")
public record SagaProperties(
        @DefaultValue("PT30S") Duration timeout,
        @DefaultValue("PT5S") Duration sweepInterval,
        @DefaultValue("50") int sweepBatchSize) {
}
