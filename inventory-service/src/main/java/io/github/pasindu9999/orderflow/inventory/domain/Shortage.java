package io.github.pasindu9999.orderflow.inventory.domain;

/** A line that can't be satisfied. {@code available} is 0 for an unknown SKU. */
public record Shortage(String sku, int requested, int available) {
}
