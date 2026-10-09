package io.github.pasindu9999.orderflow.messaging.outbox;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs the relay on its own background thread. It owns a single-thread executor instead of relying on
 * {@code @EnableScheduling}, so the library doesn't switch scheduling on for the whole application.
 * It starts after every other bean (Kafka included) and stops before them.
 */
public class OutboxRelayScheduler implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final OutboxRelay relay;
    private final OutboxProperties properties;
    private volatile ScheduledExecutorService executor;

    public OutboxRelayScheduler(OutboxRelay relay, OutboxProperties properties) {
        this.relay = relay;
        this.properties = properties;
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("outbox-relay").daemon(true).factory());
        long delay = properties.pollInterval().toMillis();
        executor.scheduleWithFixedDelay(this::drain, delay, delay, TimeUnit.MILLISECONDS);
    }

    /** Keeps relaying while batches come back full, so a burst is cleared without waiting a poll interval per batch. */
    private void drain() {
        try {
            int sent;
            do {
                sent = relay.publishBatch();
            } while (sent == properties.batchSize() && !Thread.currentThread().isInterrupted());
        } catch (RuntimeException e) {
            // Never let an exception escape: it would silently cancel all future runs of a scheduled task.
            log.warn("Outbox relay run failed; unpublished rows will be retried: {}", e.toString());
        }
    }

    @Override
    public void stop() {
        ScheduledExecutorService current = executor;
        if (current != null) {
            current.shutdownNow();
            try {
                current.awaitTermination(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
    }

    @Override
    public boolean isRunning() {
        return executor != null;
    }
}
