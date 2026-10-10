package io.github.pasindu9999.orderflow.messaging.cleanup;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pasindu9999.orderflow.messaging.outbox.MessagingTestApplication;
import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Retention of the outbox (published rows, 7 days) and the inbox (14 days). Rows are timestamped relative to the
 * database clock, a day either side of the boundary, so the test doesn't depend on exact timing.
 */
@SpringBootTest(classes = MessagingTestApplication.class)
class MessagingCleanupIT {

    @Autowired MessagingCleanup cleanup;
    @Autowired JdbcClient jdbc;

    @Test
    void shouldDeletePublishedOutboxRowsPastRetentionOnly_whenCleanupRuns() {
        UUID oldPublished = outboxRow("8 days", "8 days");
        UUID recentPublished = outboxRow("6 days", "6 days");

        MessagingCleanup.Result result = cleanup.run();

        assertThat(outboxExists(oldPublished)).isFalse();
        assertThat(outboxExists(recentPublished)).isTrue();
        assertThat(result.outboxRows()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void shouldNeverDeleteUnpublishedOutboxRows_whenTheyAreOld() {
        UUID oldUnpublished = outboxRow("30 days", null); // e.g. Kafka was down for a month: still must be sent

        cleanup.run();

        assertThat(outboxExists(oldUnpublished)).isTrue();
    }

    @Test
    void shouldDeleteInboxRowsPastRetentionOnly_whenCleanupRuns() {
        UUID old = inboxRow("15 days");
        UUID recent = inboxRow("13 days");

        MessagingCleanup.Result result = cleanup.run();

        assertThat(inboxExists(old)).isFalse();
        assertThat(inboxExists(recent)).isTrue();
        assertThat(result.inboxRows()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void shouldDeleteEveryExpiredRow_whenThereAreMoreThanOneBatch() {
        var smallBatches = new MessagingCleanup(jdbc, Clock.systemUTC(),
                new CleanupProperties(Duration.ofDays(7), Duration.ofDays(14), Duration.ofHours(1), 2, true));
        List<UUID> old = IntStream.range(0, 5).mapToObj(i -> inboxRow("20 days")).toList();

        smallBatches.run();

        assertThat(old).noneMatch(this::inboxExists);
    }

    private UUID outboxRow(String createdAgo, String publishedAgo) {
        UUID messageId = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO outbox (message_id, topic, message_key, message_type, envelope, created_at, published_at)
                        VALUES (:id, 'test.topic', :key, 'Test', '{}'::jsonb, now() - CAST(:created AS interval),
                                now() - CAST(:published AS interval))
                        """)
                .param("id", messageId)
                .param("key", UUID.randomUUID().toString())
                .param("created", createdAgo)
                .param("published", publishedAgo, Types.VARCHAR) // null: never published
                .update();
        return messageId;
    }

    private UUID inboxRow(String processedAgo) {
        UUID messageId = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO processed_message (consumer, message_id, processed_at)
                        VALUES ('cleanup-test', :id, now() - CAST(:ago AS interval))
                        """)
                .param("id", messageId)
                .param("ago", processedAgo)
                .update();
        return messageId;
    }

    private boolean outboxExists(UUID messageId) {
        return jdbc.sql("SELECT count(*) = 1 FROM outbox WHERE message_id = :id").param("id", messageId)
                .query(Boolean.class).single();
    }

    private boolean inboxExists(UUID messageId) {
        return jdbc.sql("SELECT count(*) = 1 FROM processed_message WHERE message_id = :id").param("id", messageId)
                .query(Boolean.class).single();
    }
}
