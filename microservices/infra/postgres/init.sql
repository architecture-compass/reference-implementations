-- Shared schema for every language implementation.
-- Each service owns its own database; neither reads the other's tables.

CREATE DATABASE orders;
CREATE DATABASE payments;

\connect orders

CREATE TABLE orders (
    id              UUID PRIMARY KEY,
    customer_id     TEXT          NOT NULL,
    sku             TEXT          NOT NULL,
    quantity        INT           NOT NULL CHECK (quantity > 0),
    amount          NUMERIC(12,2) NOT NULL,
    currency        TEXT          NOT NULL,
    status          TEXT          NOT NULL,          -- PENDING, CONFIRMED
    idempotency_key TEXT          NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,             -- becomes the Kafka message key
    event_type     TEXT        NOT NULL,
    topic          TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

\connect payments

CREATE TABLE payments (
    id          UUID PRIMARY KEY,
    order_id    UUID          NOT NULL UNIQUE,       -- at most one capture per order
    amount      NUMERIC(12,2) NOT NULL,
    status      TEXT          NOT NULL,              -- CAPTURED
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    topic          TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
