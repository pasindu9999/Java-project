package io.github.pasindu9999.orderflow.payment;

import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.messaging.testing.ProgrammableFaultInjector;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Real Postgres and Kafka for integration tests. {@code @ServiceConnection} wires the datasource and
 * bootstrap servers; Spring's test-context cache shares the containers across ITs with the same context.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:18.6-alpine");
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        // Same as compose: topics exist only if a service declares them.
        return new KafkaContainer("apache/kafka:4.3.1").withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }

    /** Does nothing until a test arms a fault point. Shared by all ITs so they also share one context. */
    @Bean
    ProgrammableFaultInjector faultInjector() {
        return new ProgrammableFaultInjector();
    }

    /** In the real system order-service declares this topic. Here the test plays order-service. */
    @Bean
    NewTopic paymentCommands() {
        return TopicBuilder.name(Topics.PAYMENT_COMMANDS).partitions(3).replicas(1).build();
    }
}
