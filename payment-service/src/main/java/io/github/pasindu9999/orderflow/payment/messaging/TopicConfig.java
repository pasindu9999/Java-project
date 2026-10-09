package io.github.pasindu9999.orderflow.payment.messaging;

import io.github.pasindu9999.orderflow.contracts.Topics;
import io.github.pasindu9999.orderflow.messaging.error.DeadLetterErrorHandler;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics this service produces to, plus the DLT of the topic it consumes (ARCHITECTURE §4).
 * {@code payment.commands} itself is declared by order-service, its producer; until that topic exists the listener just
 * waits for it.
 */
@Configuration(proxyBeanMethods = false)
class TopicConfig {

    static final int PARTITIONS = 3;
    /** 1 for the single local broker; production would use 3 with min.insync.replicas=2. */
    static final int REPLICAS = 1;

    @Bean
    NewTopic paymentEvents() {
        return TopicBuilder.name(Topics.PAYMENT_EVENTS).partitions(PARTITIONS).replicas(REPLICAS).build();
    }

    /** DLT of the consumed topic (ADR-0004). Same partition count, so a record keeps its partition number. */
    @Bean
    NewTopic paymentCommandsDlt() {
        return TopicBuilder.name(Topics.PAYMENT_COMMANDS + DeadLetterErrorHandler.DLT_SUFFIX).partitions(PARTITIONS).replicas(REPLICAS).build();
    }
}
