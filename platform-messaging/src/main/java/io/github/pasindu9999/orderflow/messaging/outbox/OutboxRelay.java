package io.github.pasindu9999.orderflow.messaging.outbox;

import io.github.pasindu9999.orderflow.messaging.FaultInjector;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Moves committed outbox rows to Kafka (ADR-0002). Delivery is at-least-once; consumers de-duplicate by messageId.
 *
 * <p>Each batch is one transaction:
 * <ol>
 *   <li>Take a transaction-scoped advisory lock. If another instance holds it, do nothing: one relay at a time
 *       keeps per-order ordering trivially correct.</li>
 *   <li>Read unpublished rows in {@code id} order. Selecting "not yet published" instead of "after the last id
 *       sent" matters: ids are assigned at insert, not commit, so a cursor could skip a late-committing row.</li>
 *   <li>Send each row synchronously, then mark it. A failed send stops the batch, so nothing overtakes it, and
 *       the rows already marked are kept.</li>
 * </ol>
 * If the process dies after a send but before COMMIT, the marks roll back and those rows are sent again.
 */
public class OutboxRelay {

    /** Fault point inside the send step: behaves like a failed Kafka send. */
    public static final String BEFORE_SEND = "outbox-relay.before-send";
    /** Fault point after a successful send, before the row is marked: behaves like a crash. */
    public static final String AFTER_SEND = "outbox-relay.after-send";

    /** Arbitrary but fixed. Each service has its own database, so the id only has to be unique per database. */
    static final long RELAY_LOCK_ID = 7_412_001L;

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private record OutboxRow(long id, String topic, String messageKey, String envelope) {
    }

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate transaction;
    private final OutboxProperties properties;
    private final FaultInjector faults;

    public OutboxRelay(JdbcClient jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate transaction,
                       OutboxProperties properties, FaultInjector faults) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.transaction = transaction;
        this.properties = properties;
        this.faults = faults;
    }

    /** Publishes one batch. Returns the number of rows sent and marked, or 0 if another instance holds the lock. */
    public int publishBatch() {
        Integer sent = transaction.execute(status -> {
            Boolean locked = jdbc.sql("SELECT pg_try_advisory_xact_lock(:lockId)")
                    .param("lockId", RELAY_LOCK_ID)
                    .query(Boolean.class)
                    .single();
            if (!Boolean.TRUE.equals(locked)) {
                return 0;
            }
            return sendInOrder(unpublishedRows());
        });
        return sent == null ? 0 : sent;
    }

    private int sendInOrder(List<OutboxRow> rows) {
        int sent = 0;
        for (OutboxRow row : rows) {
            try {
                faults.at(BEFORE_SEND);
                kafka.send(row.topic(), row.messageKey(), row.envelope())
                        .get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Stop here so no later row overtakes this one. Rows sent so far stay marked.
                log.warn("Outbox relay stopped at row {} ({}): {}. It will be retried.", row.id(), row.topic(), e.toString());
                break;
            }
            faults.at(AFTER_SEND);
            jdbc.sql("UPDATE outbox SET published_at = now() WHERE id = :id").param("id", row.id()).update();
            sent++;
        }
        return sent;
    }

    private List<OutboxRow> unpublishedRows() {
        // The envelope is stored as JSONB, which normalises key order and whitespace. The text sent is therefore
        // the same JSON, not the same bytes, which is fine for any JSON consumer.
        return jdbc.sql("""
                        SELECT id, topic, message_key, CAST(envelope AS text) AS envelope
                        FROM outbox
                        WHERE published_at IS NULL
                        ORDER BY id
                        LIMIT :batchSize
                        """)
                .param("batchSize", properties.batchSize())
                .query((rs, rowNum) -> new OutboxRow(
                        rs.getLong("id"), rs.getString("topic"), rs.getString("message_key"), rs.getString("envelope")))
                .list();
    }
}
