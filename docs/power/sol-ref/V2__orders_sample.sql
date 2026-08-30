-- V2 — таблицы sample приложения Orders.Api (idempotent)
CREATE TABLE IF NOT EXISTS app_order (
    id              uuid PRIMARY KEY,
    customer_id     uuid NOT NULL,
    total           numeric(18,2) NOT NULL,
    currency        text NOT NULL,
    status          text NOT NULL DEFAULT 'created',
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS order_projection (
    order_id        uuid PRIMARY KEY,
    customer_id     uuid NOT NULL,
    total           numeric(18,2) NOT NULL,
    currency        text NOT NULL,
    projected_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO avtobus.schema_version(version, description)
VALUES (2, 'Orders sample tables')
ON CONFLICT (version) DO NOTHING;
