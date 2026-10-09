package io.github.pasindu9999.orderflow.order.config;

import io.github.pasindu9999.orderflow.order.app.OrderTimeoutSweeper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Runs the timeout sweeper in the background. Tests switch it off ({@code orderflow.saga.sweeper-enabled=false})
 * and call {@link OrderTimeoutSweeper#sweep()} themselves, so every step is deterministic. A failing run is logged
 * by the scheduler and simply retried on the next tick.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnBooleanProperty(name = "orderflow.saga.sweeper-enabled", matchIfMissing = true)
class TimeoutSweepSchedule {

    private final OrderTimeoutSweeper sweeper;

    TimeoutSweepSchedule(OrderTimeoutSweeper sweeper) {
        this.sweeper = sweeper;
    }

    @Scheduled(fixedDelayString = "${orderflow.saga.sweep-interval:PT5S}")
    void sweep() {
        sweeper.sweep();
    }
}
