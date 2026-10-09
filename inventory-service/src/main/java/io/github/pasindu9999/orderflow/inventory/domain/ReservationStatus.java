package io.github.pasindu9999.orderflow.inventory.domain;

/** Every state is final for its order: a reservation row is never deleted, so a replayed command finds it. */
public enum ReservationStatus {
    RESERVED,
    REJECTED,
    RELEASED
}
