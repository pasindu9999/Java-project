-- Same messaging tables every service creates in its own V1 migration (ARCHITECTURE §6.1).
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

CREATE TABLE processed_message (
    consumer      TEXT        NOT NULL,
    message_id    UUID        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);
