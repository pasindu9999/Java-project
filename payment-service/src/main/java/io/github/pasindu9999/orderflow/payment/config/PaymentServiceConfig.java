package io.github.pasindu9999.orderflow.payment.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class PaymentServiceConfig {

    /** Injected everywhere time matters (command expiry), so tests can control it. */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
