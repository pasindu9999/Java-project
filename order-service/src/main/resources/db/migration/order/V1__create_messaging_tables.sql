-- Transactional outbox (ADR-0002): rows are written in the same transaction as the business change
-- and published to Kafka by the relay. published_at IS NULL means "not yet sent".
CREATE TABLE outbox (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id    UUID        NOT NULL UNIQUE,
    topic         TEXT        NOT NULL,
    message_key   TEXT        NOT NULL,
    message_type  TEXT        NOT NULL,
    envelope      JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Inbox / processed-message tracking (ADR-0003): inserted first, in the same transaction as the handler.
CREATE TABLE processed_message (
    consumer      TEXT        NOT NULL,
    message_id    UUID        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
