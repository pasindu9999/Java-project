package io.github.pasindu9999.orderflow.order.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.github.pasindu9999.orderflow.order.domain.OrderDraft.LineInput;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OrderDraftTest {

    final UUID customerId = UUID.randomUUID();

    @Test
    void shouldNormaliseLinesAndComputeTotal_whenInputIsValid() {
        OrderDraft draft = OrderDraft.of(customerId, "EUR", List.of(
                new LineInput(" MUG-RED ", 2, new BigDecimal("12.5")),
                new LineInput("TEA-GREEN", 1, new BigDecimal("12.50"))));

        assertThat(draft.lines()).containsExactly(
                new OrderLine(1, "MUG-RED", 2, new BigDecimal("12.50")),
                new OrderLine(2, "TEA-GREEN", 1, new BigDecimal("12.50")));
        assertThat(draft.total()).isEqualTo(new BigDecimal("37.50"));
    }

    @Test
    void shouldReportEveryViolation_whenSeveralRulesAreBroken() {
        var e = catchThrowableOfType(InvalidOrderException.class, () -> OrderDraft.of(null, "eur", List.of(
                new LineInput("", 0, new BigDecimal("-1")),
                new LineInput("OK", 1, new BigDecimal("1.999")))));

        assertThat(e.violations()).containsExactly(
                "customerId is required",
                "currency must be an ISO 4217 code such as EUR, got 'eur'",
                "lines[0].sku is required",
                "lines[0].quantity must be between 1 and 1000",
                "lines[0].unitPrice must not be negative",
                "lines[1].unitPrice must have at most 2 decimal places");
    }

    @Test
    void shouldRejectMissingLineFields_whenClientSendsNulls() {
        var e = catchThrowableOfType(InvalidOrderException.class,
                () -> OrderDraft.of(customerId, "EUR", Collections.singletonList(new LineInput(null, null, null))));

        assertThat(e.violations()).containsExactly(
                "lines[0].sku is required", "lines[0].quantity is required", "lines[0].unitPrice is required");
    }

    @Test
    void shouldRejectOrder_whenThereAreNoLines() {
        assertThatThrownBy(() -> OrderDraft.of(customerId, "EUR", List.of()))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("lines must contain at least one line");
        assertThatThrownBy(() -> OrderDraft.of(customerId, "EUR", null))
                .isInstanceOf(InvalidOrderException.class);
    }

    @Test
    void shouldRejectOrder_whenThereAreTooManyLines() {
        List<LineInput> lines = new ArrayList<>();
        for (int i = 0; i <= OrderDraft.MAX_LINES; i++) {
            lines.add(new LineInput("SKU-" + i, 1, BigDecimal.ONE));
        }

        assertThatThrownBy(() -> OrderDraft.of(customerId, "EUR", lines))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("at most 50 lines");
    }

    @Test
    void shouldRejectOrder_whenTotalIsZero() {
        assertThatThrownBy(() -> OrderDraft.of(customerId, "EUR", List.of(new LineInput("FREEBIE", 3, BigDecimal.ZERO))))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("order total must be greater than 0");
    }

    @Test
    void shouldRejectUnknownCurrency_whenCodeIsWellFormedButNotIso() {
        assertThatThrownBy(() -> OrderDraft.of(customerId, "XYZ", List.of(new LineInput("A", 1, BigDecimal.ONE))))
                .isInstanceOf(InvalidOrderException.class)
                .hasMessageContaining("ISO 4217");
    }

    @Test
    void shouldProduceSameFingerprint_whenRequestsDifferOnlyInFormatting() {
        OrderDraft a = OrderDraft.of(customerId, "EUR", List.of(new LineInput("MUG-RED", 2, new BigDecimal("12.5"))));
        OrderDraft b = OrderDraft.of(customerId, "EUR", List.of(new LineInput(" MUG-RED", 2, new BigDecimal("12.500"))));

        assertThat(a.fingerprint()).isEqualTo(b.fingerprint()).hasSize(64);
    }

    @Test
    void shouldProduceDifferentFingerprint_whenAnyMeaningfulFieldChanges() {
        String base = draft(customerId, "MUG-RED", 2, "12.50").fingerprint();

        assertThat(draft(UUID.randomUUID(), "MUG-RED", 2, "12.50").fingerprint()).isNotEqualTo(base);
        assertThat(draft(customerId, "MUG-BLUE", 2, "12.50").fingerprint()).isNotEqualTo(base);
        assertThat(draft(customerId, "MUG-RED", 3, "12.50").fingerprint()).isNotEqualTo(base);
        assertThat(draft(customerId, "MUG-RED", 2, "12.51").fingerprint()).isNotEqualTo(base);
    }

    private static OrderDraft draft(UUID customerId, String sku, int quantity, String price) {
        return OrderDraft.of(customerId, "EUR", List.of(new LineInput(sku, quantity, new BigDecimal(price))));
    }
}
