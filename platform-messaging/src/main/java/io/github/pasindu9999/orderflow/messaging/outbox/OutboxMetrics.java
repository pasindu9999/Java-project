package io.github.pasindu9999.orderflow.messaging.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Outbox health as gauges. A growing {@code outbox.oldest.age} is the alert signal: Kafka is down or the relay
 * is stuck, and every saga waiting on those messages is stalled.
 */
public class OutboxMetrics implements MeterBinder {

    private static final Logger log = LoggerFactory.getLogger(OutboxMetrics.class);

    private final JdbcClient jdbc;

    public OutboxMetrics(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("outbox.pending", this, OutboxMetrics::pending)
                .description("Outbox rows not yet published to Kafka")
                .register(registry);
        Gauge.builder("outbox.oldest.age", this, OutboxMetrics::oldestAgeSeconds)
                .description("Age of the oldest unpublished outbox row (0 when empty)")
                .baseUnit("seconds")
                .register(registry);
    }

    double pending() {
        return query("SELECT count(*) FROM outbox WHERE published_at IS NULL");
    }

    double oldestAgeSeconds() {
        // Measured with the database clock on both ends, so application clock skew can't distort it.
        return query("""
                SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
                FROM outbox WHERE published_at IS NULL
                """);
    }

    private double query(String sql) {
        try {
            return jdbc.sql(sql).query(Double.class).single();
        } catch (RuntimeException e) {
            log.debug("Outbox gauge query failed", e);
            return Double.NaN;
        }
    }
}
