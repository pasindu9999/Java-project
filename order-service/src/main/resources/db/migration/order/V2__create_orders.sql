-- Orders and their saga state (ARCHITECTURE §3.2, §6.2). The status column is the saga state machine.
CREATE TABLE orders (
    id               UUID PRIMARY KEY,
    customer_id      UUID          NOT NULL,
    status           TEXT          NOT NULL CHECK (status IN ('PENDING', 'AWAITING_PAYMENT', 'CONFIRMED', 'CANCELLED')),
    cancel_reason    TEXT,
    total_amount     NUMERIC(19,2) NOT NULL CHECK (total_amount > 0),
    currency         CHAR(3)       NOT NULL,
    idempotency_key  TEXT          NOT NULL,
    request_hash     TEXT          NOT NULL,   -- detects the same key reused with a different body
    deadline_at      TIMESTAMPTZ   NOT NULL,   -- saga timeout (ARCHITECTURE §7.4)
    created_at       TIMESTAMPTZ   NOT NULL,
    updated_at       TIMESTAMPTZ   NOT NULL,
    version          BIGINT        NOT NULL DEFAULT 0,
    -- Keys are scoped per customer, and the index makes concurrent duplicates fail fast.
    UNIQUE (customer_id, idempotency_key),
    CHECK ((status = 'CANCELLED') = (cancel_reason IS NOT NULL))
);

-- Only open orders can time out, so the sweeper's index stays small.
CREATE INDEX orders_open_deadline_idx ON orders (deadline_at)
    WHERE status IN ('PENDING', 'AWAITING_PAYMENT');

CREATE TABLE order_lines (
    order_id    UUID          NOT NULL REFERENCES orders (id),
    line_no     INT           NOT NULL,
    sku         TEXT          NOT NULL,
    quantity    INT           NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(19,2) NOT NULL CHECK (unit_price >= 0),
    PRIMARY KEY (order_id, line_no)
);
