package io.github.pasindu9999.orderflow.payment.config;

import io.github.pasindu9999.orderflow.payment.domain.PaymentPolicy;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Simulator rules (ARCHITECTURE §6.4). */
@ConfigurationProperties("orderflow.payment")
public record PaymentProperties(
        @DefaultValue List<UUID> blockedCustomers,
        @DefaultValue("1000.00") BigDecimal declineAbove,
        @DefaultValue("13.13") BigDecimal transientFailureAmount,
        @DefaultValue("2") int transientFailures) {

    public PaymentPolicy.Rules rules() {
        return new PaymentPolicy.Rules(Set.copyOf(blockedCustomers), declineAbove, transientFailureAmount, transientFailures);
    }
}
