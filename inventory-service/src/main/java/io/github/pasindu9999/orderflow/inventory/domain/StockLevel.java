package io.github.pasindu9999.orderflow.inventory.domain;

/** @param reserved units held for orders still in progress; {@code available} excludes them */
public record StockLevel(String sku, int available, int reserved) {
}
