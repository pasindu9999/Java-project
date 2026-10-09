package io.github.pasindu9999.orderflow.messaging.outbox;

import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.messaging.testing.ProgrammableFaultInjector;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Minimal application for testing the outbox mechanics on their own, against real Postgres and Kafka.
 * The relay's background thread is off (see application.yml): tests call {@code publishBatch()} themselves.
 */
@SpringBootApplication
class MessagingTestApplication {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:18.6-alpine");
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        // Same as compose: a topic that isn't declared in code must not appear by accident.
        return new KafkaContainer("apache/kafka:4.3.1").withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }

    @Bean
    ProgrammableFaultInjector faultInjector() {
        return new ProgrammableFaultInjector();
    }

    @Bean
    NewTopic inventoryCommands() {
        return TopicBuilder.name(Topics.INVENTORY_COMMANDS).partitions(3).replicas(1).build();
    }
}
