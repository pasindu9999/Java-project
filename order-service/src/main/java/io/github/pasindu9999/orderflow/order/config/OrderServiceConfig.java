package io.github.pasindu9999.orderflow.order.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class OrderServiceConfig {

    /** Injected everywhere time matters, so tests can control it. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
