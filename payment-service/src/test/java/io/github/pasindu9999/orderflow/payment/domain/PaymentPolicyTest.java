package io.github.pasindu9999.orderflow.payment.domain;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.pasindu9999.orderflow.payment.domain.PaymentPolicy.Approve;
import io.github.pasindu9999.orderflow.payment.domain.PaymentPolicy.Decline;
import io.github.pasindu9999.orderflow.payment.domain.PaymentPolicy.ProviderUnavailable;
import java.math.BigDecimal;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PaymentPolicyTest {

    final UUID customer = UUID.randomUUID();
    final UUID blocked = UUID.randomUUID();
    final PaymentPolicy.Rules rules = new PaymentPolicy.Rules(
            Set.of(blocked), new BigDecimal("1000.00"), new BigDecimal("13.13"), 2);

    @Test
    void shouldApprove_whenNoRuleApplies() {
        assertThat(decide(customer, "37.50", 1)).isEqualTo(new Approve());
    }

    @Test
    void shouldApprove_whenAmountIsExactlyTheLimit() {
        assertThat(decide(customer, "1000.00", 1)).isEqualTo(new Approve());
    }

    @Test
    void shouldDeclineForLimit_whenAmountIsAboveIt() {
        assertThat(decide(customer, "1000.01", 1)).isEqualTo(new Decline(DeclineReason.DECLINED_LIMIT));
    }

    @Test
    void shouldReportBlockedCustomerFirst_whenCustomerIsBlockedAndAmountIsAboveLimit() {
        assertThat(decide(blocked, "5000.00", 1)).isEqualTo(new Decline(DeclineReason.DECLINED_BLOCKED_CUSTOMER));
    }

    @Test
    void shouldBeUnavailableThenApprove_whenAmountIsTheFlakyAmount() {
        assertThat(decide(customer, "13.13", 1)).isEqualTo(new ProviderUnavailable());
        assertThat(decide(customer, "13.130", 2)).as("compared by value, not scale").isEqualTo(new ProviderUnavailable());
        assertThat(decide(customer, "13.13", 3)).isEqualTo(new Approve());
    }

    private PaymentPolicy.Decision decide(UUID customerId, String amount, int attempt) {
        return PaymentPolicy.decide(customerId, new BigDecimal(amount), attempt, rules);
    }
}
