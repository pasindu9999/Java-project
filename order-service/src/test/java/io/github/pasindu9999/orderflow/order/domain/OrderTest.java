package io.github.pasindu9999.orderflow.order.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class OrderTest {

    static final Instant NOW = Instant.parse("2026-10-09T08:00:00Z");
    static final Instant LATER = NOW.plusSeconds(5);

    final OrderDraft draft = OrderDraft.of(UUID.randomUUID(), "EUR",
            List.of(new OrderDraft.LineInput("MUG-RED", 3, new BigDecimal("10.00"))));

    @Test
    void shouldStartPendingWithDeadline_whenPlaced() {
        Order order = Order.place(UUID.randomUUID(), draft, "key-1", NOW, Duration.ofSeconds(30));

        assertThat(order.status()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.cancelReason()).isNull();
        assertThat(order.totalAmount()).isEqualTo(new BigDecimal("30.00"));
        assertThat(order.deadlineAt()).isEqualTo(NOW.plusSeconds(30));
        assertThat(order.requestHash()).isEqualTo(draft.fingerprint());
        assertThat(order.version()).isZero();
    }

    @Test
    void shouldFollowHappyPath_whenInventoryAndPaymentSucceed() {
        Order confirmed = pending().awaitPayment(NOW).confirm(LATER);

        assertThat(confirmed.status()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(confirmed.updatedAt()).isEqualTo(LATER);
        assertThat(confirmed.createdAt()).isEqualTo(NOW);
    }

    @Test
    void shouldRecordReason_whenCancelled() {
        Order cancelled = pending().awaitPayment(NOW).cancel(CancelReason.PAYMENT_DECLINED, LATER);

        assertThat(cancelled.status()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(cancelled.cancelReason()).isEqualTo(CancelReason.PAYMENT_DECLINED);
    }

    @Test
    void shouldRequireReason_whenCancelling() {
        assertThatThrownBy(() -> pending().cancel(null, NOW)).isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @CsvSource({
            "PENDING, CONFIRMED",          // can't confirm before inventory and payment
            "AWAITING_PAYMENT, AWAITING_PAYMENT",
            "CONFIRMED, CANCELLED",        // terminal
            "CANCELLED, CONFIRMED",        // terminal: a late payment triggers a refund, not a confirm
            "CANCELLED, AWAITING_PAYMENT"})
    void shouldRejectTransition_whenStateMachineDoesNotAllowIt(OrderStatus from, OrderStatus to) {
        Order order = inState(from);

        assertThatThrownBy(() -> {
            Order unused = switch (to) {
                case PENDING -> throw new AssertionError("no transition targets PENDING");
                case AWAITING_PAYMENT -> order.awaitPayment(NOW);
                case CONFIRMED -> order.confirm(NOW);
                case CANCELLED -> order.cancel(CancelReason.TIMEOUT, NOW);
            };
        }).isInstanceOf(IllegalOrderTransitionException.class);
    }

    @ParameterizedTest
    @EnumSource(OrderStatus.class)
    void shouldNotLeaveTerminalState_whenAnyTransitionIsAttempted(OrderStatus status) {
        assertThat(status.isTerminal()).isEqualTo(
                List.of(OrderStatus.values()).stream().noneMatch(status::canTransitionTo));
    }

    @Test
    void shouldRejectIdempotencyKey_whenBlankOrTooLong() {
        assertThatThrownBy(() -> Order.place(UUID.randomUUID(), draft, " ", NOW, Duration.ofSeconds(30)))
                .isInstanceOf(InvalidOrderException.class);
        assertThatThrownBy(() -> Order.place(UUID.randomUUID(), draft, "k".repeat(101), NOW, Duration.ofSeconds(30)))
                .isInstanceOf(InvalidOrderException.class);
    }

    private Order pending() {
        return Order.place(UUID.randomUUID(), draft, "key-1", NOW, Duration.ofSeconds(30));
    }

    private Order inState(OrderStatus status) {
        return switch (status) {
            case PENDING -> pending();
            case AWAITING_PAYMENT -> pending().awaitPayment(NOW);
            case CONFIRMED -> pending().awaitPayment(NOW).confirm(NOW);
            case CANCELLED -> pending().cancel(CancelReason.OUT_OF_STOCK, NOW);
        };
    }
}
