package io.github.pasindu9999.orderflow.messaging.cleanup;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs {@link MessagingCleanup} on its own background thread, like the outbox relay: the library never switches
 * {@code @EnableScheduling} on for the whole application.
 */
public class MessagingCleanupScheduler implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MessagingCleanupScheduler.class);

    private final MessagingCleanup cleanup;
    private final CleanupProperties properties;
    private volatile ScheduledExecutorService executor;

    public MessagingCleanupScheduler(MessagingCleanup cleanup, CleanupProperties properties) {
        this.cleanup = cleanup;
        this.properties = properties;
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("messaging-cleanup").daemon(true).factory());
        long interval = properties.interval().toMillis();
        executor.scheduleWithFixedDelay(this::runSafely, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void runSafely() {
        try {
            cleanup.run();
        } catch (RuntimeException e) {
            // Never let an exception escape: it would silently cancel all future runs of a scheduled task.
            log.warn("Messaging cleanup failed; it will run again in {}: {}", properties.interval(), e.toString());
        }
    }

    @Override
    public void stop() {
        ScheduledExecutorService current = executor;
        if (current != null) {
            current.shutdownNow();
            executor = null;
        }
    }

    @Override
    public boolean isRunning() {
        return executor != null;
    }
}
