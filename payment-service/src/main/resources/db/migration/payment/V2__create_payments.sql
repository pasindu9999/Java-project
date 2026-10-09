-- Payments (ARCHITECTURE §6.4). A FAILED charge is a row too, so a replayed ProcessPayment finds it.
CREATE TABLE payment (
    id              UUID PRIMARY KEY,
    order_id        UUID          NOT NULL UNIQUE,   -- at most one charge per order
    customer_id     UUID          NOT NULL,
    amount          NUMERIC(19,2) NOT NULL CHECK (amount > 0),
    currency        CHAR(3)       NOT NULL,
    status          TEXT          NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED', 'REFUNDED')),
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CHECK ((status = 'FAILED') = (failure_reason IS NOT NULL))
);
