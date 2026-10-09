package io.github.pasindu9999.orderflow.messaging.autoconfigure;

import io.github.pasindu9999.orderflow.contracts.MessageCatalog;
import io.github.pasindu9999.orderflow.messaging.FaultInjector;
import io.github.pasindu9999.orderflow.messaging.MessageCodec;
import io.github.pasindu9999.orderflow.messaging.error.ConsumerRetryProperties;
import io.github.pasindu9999.orderflow.messaging.error.DeadLetterErrorHandler;
import io.github.pasindu9999.orderflow.messaging.inbox.IdempotentMessageHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.github.pasindu9999.orderflow.messaging.outbox.OutboxMetrics;
import io.github.pasindu9999.orderflow.messaging.outbox.OutboxProperties;
import io.github.pasindu9999.orderflow.messaging.outbox.OutboxRelay;
import io.github.pasindu9999.orderflow.messaging.outbox.OutboxRelayScheduler;
import io.github.pasindu9999.orderflow.messaging.outbox.OutboxWriter;
import java.time.Clock;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Gives every service the same codec, outbox writer and relay, idempotent inbox handler and consumer error
 * handler. Registered in {@code AutoConfiguration.imports}.
 */
@AutoConfiguration
@EnableConfigurationProperties({OutboxProperties.class, ConsumerRetryProperties.class})
public class MessagingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    FaultInjector faultInjector() {
        return FaultInjector.NONE;
    }

    @Bean
    @ConditionalOnMissingBean
    MessageCodec messageCodec(Environment environment, ObjectProvider<Clock> clock) {
        String producer = environment.getRequiredProperty("spring.application.name");
        return new MessageCodec(MessageCatalog.ENTRIES, producer, clock.getIfAvailable(Clock::systemUTC));
    }

    @Bean
    OutboxWriter outboxWriter(JdbcClient jdbc, MessageCodec codec, FaultInjector faults) {
        return new OutboxWriter(jdbc, codec, faults);
    }

    @Bean
    OutboxRelay outboxRelay(JdbcClient jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate transaction,
                            OutboxProperties properties, FaultInjector faults) {
        return new OutboxRelay(jdbc, kafka, transaction, properties, faults);
    }

    @Bean
    @ConditionalOnBooleanProperty(name = "orderflow.outbox.relay-enabled", matchIfMissing = true)
    OutboxRelayScheduler outboxRelayScheduler(OutboxRelay relay, OutboxProperties properties) {
        return new OutboxRelayScheduler(relay, properties);
    }

    @Bean
    OutboxMetrics outboxMetrics(JdbcClient jdbc) {
        return new OutboxMetrics(jdbc);
    }

    @Bean
    IdempotentMessageHandler idempotentMessageHandler(JdbcClient jdbc, TransactionTemplate transaction,
                                                      MessageCodec codec, ObjectProvider<MeterRegistry> meters) {
        return new IdempotentMessageHandler(jdbc, transaction, codec, meters.getIfAvailable(SimpleMeterRegistry::new));
    }

    /** Spring Boot applies a single {@link CommonErrorHandler} bean to every listener container. */
    @Bean
    @ConditionalOnMissingBean(CommonErrorHandler.class)
    CommonErrorHandler deadLetterErrorHandler(KafkaTemplate<String, String> kafka, ConsumerRetryProperties retry,
                                              ObjectProvider<MeterRegistry> meters) {
        return DeadLetterErrorHandler.create(kafka, retry, meters.getIfAvailable(SimpleMeterRegistry::new));
    }
}
