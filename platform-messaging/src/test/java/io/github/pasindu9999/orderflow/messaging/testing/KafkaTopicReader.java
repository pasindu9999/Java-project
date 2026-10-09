package io.github.pasindu9999.orderflow.messaging.testing;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Reads everything currently in a topic, from the beginning of every partition up to the end offsets.
 *
 * <p>It assigns partitions directly instead of joining a consumer group, so there is no rebalance to wait for,
 * and it doesn't commit offsets, so it never affects the services' consumer groups. Tests filter the result by
 * key (orderId), because topics are shared across tests.
 */
public final class KafkaTopicReader {

    private static final Duration MAX_READ_TIME = Duration.ofSeconds(15);

    private KafkaTopicReader() {
    }

    /** @param clientConfig any Kafka client config with {@code bootstrap.servers}, e.g. {@code KafkaAdmin}'s */
    public static List<ConsumerRecord<String, String>> readAll(Map<String, Object> clientConfig, String topic) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clientConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic, MAX_READ_TIME).stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions, MAX_READ_TIME);

            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            Instant deadline = Instant.now().plus(MAX_READ_TIME);
            while (partitions.stream().anyMatch(tp -> consumer.position(tp) < endOffsets.get(tp))) {
                if (Instant.now().isAfter(deadline)) {
                    throw new IllegalStateException("Timed out reading " + topic);
                }
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
            return records;
        }
    }

    /** Only the records for one key (orderId), in partition order, which for one key is the order they were sent in. */
    public static List<ConsumerRecord<String, String>> readKey(Map<String, Object> clientConfig, String topic, String key) {
        return readAll(clientConfig, topic).stream()
                .filter(r -> key.equals(r.key()))
                .sorted(Comparator.comparingLong(ConsumerRecord::offset))
                .toList();
    }
}
