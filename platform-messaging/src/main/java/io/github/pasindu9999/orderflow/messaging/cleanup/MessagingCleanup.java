package io.github.pasindu9999.orderflow.messaging.cleanup;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Keeps the outbox and the inbox from growing forever. Deletes in small batches, each committing on its own,
 * so the relay and the consumers are never blocked behind one big DELETE.
 */
public class MessagingCleanup {

    /** Rows deleted by one run. */
    public record Result(int outboxRows, int inboxRows) {
    }

    private static final Logger log = LoggerFactory.getLogger(MessagingCleanup.class);

    private final JdbcClient jdbc;
    private final Clock clock;
    private final CleanupProperties properties;

    public MessagingCleanup(JdbcClient jdbc, Clock clock, CleanupProperties properties) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.properties = properties;
    }

    public Result run() {
        Instant now = clock.instant();
        // published_at < cutoff is never true for NULL, so an unpublished row is never deleted, however old.
        int outbox = deleteInBatches("""
                DELETE FROM outbox WHERE id IN (
                    SELECT id FROM outbox WHERE published_at < :cutoff ORDER BY id LIMIT :batch)
                """, now.minus(properties.outboxRetention()));
        int inbox = deleteInBatches("""
                DELETE FROM processed_message WHERE (consumer, message_id) IN (
                    SELECT consumer, message_id FROM processed_message WHERE processed_at < :cutoff LIMIT :batch)
                """, now.minus(properties.inboxRetention()));
        if (outbox + inbox > 0) {
            log.info("Cleanup deleted {} published outbox row(s) and {} processed_message row(s)", outbox, inbox);
        }
        return new Result(outbox, inbox);
    }

    private int deleteInBatches(String sql, Instant cutoff) {
        int total = 0;
        int deleted;
        do {
            deleted = jdbc.sql(sql)
                    .param("cutoff", cutoff.atOffset(ZoneOffset.UTC))
                    .param("batch", properties.batchSize())
                    .update();
            total += deleted;
        } while (deleted == properties.batchSize());
        return total;
    }
}
