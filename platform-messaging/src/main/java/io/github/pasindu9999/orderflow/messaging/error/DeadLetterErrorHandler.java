package io.github.pasindu9999.orderflow.messaging.error;

import io.github.pasindu9999.orderflow.messaging.MessageParseException;
import io.github.pasindu9999.orderflow.messaging.NonRetryableMessageException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * The one consumer error handler every service uses (ADR-0004): blocking retries with exponential backoff, then
 * the dead-letter topic. Blocking keeps per-order ordering: nothing behind a failing record on its partition
 * overtakes it.
 *
 * <ul>
 *   <li>{@link MessageParseException} and {@link NonRetryableMessageException} go to the DLT at once: retrying
 *       can't fix them.</li>
 *   <li>Everything else is retried in place, then dead-lettered.</li>
 *   <li>The record goes to {@code <topic>.DLT}, same partition, with Spring Kafka's diagnostic headers (original
 *       topic/partition/offset, exception class, message and stack trace). The value is untouched, so it can be
 *       replayed as is.</li>
 *   <li>The offset is only committed once the DLT publish succeeded. If that publish fails, the record is
 *       retried rather than lost.</li>
 * </ul>
 */
public final class DeadLetterErrorHandler {

    public static final String DLT_SUFFIX = ".DLT";

    private static final Logger log = LoggerFactory.getLogger(DeadLetterErrorHandler.class);

    private DeadLetterErrorHandler() {
    }

    public static DefaultErrorHandler create(KafkaOperations<?, ?> kafka, ConsumerRetryProperties retry, MeterRegistry meters) {
        DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(kafka,
                (record, exception) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));

        ConsumerRecordRecoverer recoverer = (record, exception) -> {
            deadLetters.accept(record, exception); // throws if the publish fails: not committed, so retried
            Counter.builder("messaging.dlt.published")
                    .description("Records parked in a dead-letter topic; any increase needs investigating")
                    .tag("topic", record.topic())
                    .register(meters)
                    .increment();
            // The listener's exception, unwrapped from the container's ListenerExecutionFailedException; the full
            // chain is in the DLT record's stack-trace header.
            Throwable cause = exception.getCause() == null ? exception : exception.getCause();
            log.error("Sent {}-{}@{} (key {}) to {}{}: {}", record.topic(), record.partition(), record.offset(),
                    record.key(), record.topic(), DLT_SUFFIX, cause.toString());
        };

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(MessageParseException.class, NonRetryableMessageException.class);
        return handler;
    }
}
