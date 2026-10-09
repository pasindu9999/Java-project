package io.github.pasindu9999.orderflow.order.messaging;

import io.github.pasindu9999.orderflow.contracts.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics this service produces to. A topic is declared by its producer (ARCHITECTURE §4), and the broker never
 * auto-creates topics, so a missing declaration fails loudly instead of creating a 1-partition topic by accident.
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
}
