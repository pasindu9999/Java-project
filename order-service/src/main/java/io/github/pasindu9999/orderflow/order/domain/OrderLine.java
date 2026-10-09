package io.github.pasindu9999.orderflow.order.domain;

import java.math.BigDecimal;

/** One validated order line. {@code unitPrice} always has scale 2. */
public record OrderLine(int lineNo, String sku, int quantity, BigDecimal unitPrice) {

    public BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
