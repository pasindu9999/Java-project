package io.github.pasindu9999.orderflow.inventory.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.pasindu9999.orderflow.inventory.domain.ReservationPolicy.Accept;
import io.github.pasindu9999.orderflow.inventory.domain.ReservationPolicy.Reject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReservationPolicyTest {

    final Map<String, StockLevel> stock = Map.of(
            "MUG-RED", new StockLevel("MUG-RED", 5, 0),
            "TEA-GREEN", new StockLevel("TEA-GREEN", 1, 0));

    @Test
    void shouldMergeDuplicateSkusAndSortThem_whenNormalising() {
        List<ReservationLine> normalised = ReservationPolicy.normalise(List.of(
                new ReservationLine("TEA-GREEN", 1),
                new ReservationLine("MUG-RED", 2),
                new ReservationLine("MUG-RED", 1)));

        assertThat(normalised).containsExactly(new ReservationLine("MUG-RED", 3), new ReservationLine("TEA-GREEN", 1));
    }

    @Test
    void shouldRejectMalformedRequest_whenNormalising() {
        assertThatThrownBy(() -> ReservationPolicy.normalise(List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ReservationPolicy.normalise(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ReservationPolicy.normalise(List.of(new ReservationLine(" ", 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ReservationPolicy.normalise(List.of(new ReservationLine("MUG-RED", 0))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ReservationPolicy.normalise(Arrays.asList((ReservationLine) null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldAcceptEveryLine_whenAllStockIsAvailable() {
        var lines = List.of(new ReservationLine("MUG-RED", 5), new ReservationLine("TEA-GREEN", 1));

        assertThat(ReservationPolicy.decide(lines, stock)).isEqualTo(new Accept(lines));
    }

    @Test
    void shouldRejectWholeOrder_whenOneLineIsShort() {
        var lines = List.of(new ReservationLine("MUG-RED", 1), new ReservationLine("TEA-GREEN", 2));

        assertThat(ReservationPolicy.decide(lines, stock)).isEqualTo(
                new Reject(RejectionReason.OUT_OF_STOCK, List.of(new Shortage("TEA-GREEN", 2, 1))));
    }

    @Test
    void shouldReportUnknownSkuFirst_whenSkuIsMissingAndAnotherIsShort() {
        var lines = List.of(new ReservationLine("LAMP", 1), new ReservationLine("TEA-GREEN", 2));

        assertThat(ReservationPolicy.decide(lines, stock)).isEqualTo(
                new Reject(RejectionReason.UNKNOWN_SKU, List.of(new Shortage("LAMP", 1, 0))));
    }
}
