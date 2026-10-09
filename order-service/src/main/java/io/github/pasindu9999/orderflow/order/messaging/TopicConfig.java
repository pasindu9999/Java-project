package io.github.pasindu9999.orderflow.order.messaging;

import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.messaging.error.DeadLetterErrorHandler;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics this service produces to, plus the DLTs of the topics it consumes. A topic is declared by its producer
 * (ARCHITECTURE §4), and the broker never auto-creates topics, so a missing declaration fails loudly instead of
 * creating a 1-partition topic by accident.
 */
@Configuration(proxyBeanMethods = false)
class TopicConfig {

    static final int PARTITIONS = 3;
    /** 1 for the single local broker; production would use 3 with min.insync.replicas=2. */
    static final int REPLICAS = 1;

    @Bean
    NewTopic inventoryCommands() {
        return TopicBuilder.name(Topics.INVENTORY_COMMANDS).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    NewTopic paymentCommands() {
        return TopicBuilder.name(Topics.PAYMENT_COMMANDS).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    /** DLT of a consumed topic (ADR-0004). Same partition count, so a record keeps its partition number. */
    @Bean
    NewTopic inventoryEventsDlt() {
        return TopicBuilder.name(Topics.INVENTORY_EVENTS + DeadLetterErrorHandler.DLT_SUFFIX).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    /** DLT of a consumed topic (ADR-0004). Same partition count, so a record keeps its partition number. */
    @Bean
    NewTopic paymentEventsDlt() {
        return TopicBuilder.name(Topics.PAYMENT_EVENTS + DeadLetterErrorHandler.DLT_SUFFIX).partitions(PARTITIONS).replicas(REPLICAS).build();
    }
}
