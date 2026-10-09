-- Stock and reservations (ARCHITECTURE §6.3).
CREATE TABLE product_stock (
    sku         TEXT PRIMARY KEY,
    -- The CHECKs are a safety net: the service validates under row locks before it changes anything.
    available   INT NOT NULL CHECK (available >= 0),
    reserved    INT NOT NULL CHECK (reserved >= 0),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per order: natural idempotency for ReserveInventory, and the tombstone for ReleaseInventory.
CREATE TABLE reservation (
    order_id    UUID PRIMARY KEY,
    status      TEXT NOT NULL CHECK (status IN ('RESERVED', 'REJECTED', 'RELEASED')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE reservation_line (
    order_id  UUID NOT NULL REFERENCES reservation (order_id),
    sku       TEXT NOT NULL REFERENCES product_stock (sku),
    quantity  INT  NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, sku)
);
