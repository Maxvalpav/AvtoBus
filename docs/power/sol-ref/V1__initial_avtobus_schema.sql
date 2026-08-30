BEGIN;

CREATE SCHEMA IF NOT EXISTS avtobus;

CREATE TABLE IF NOT EXISTS avtobus.schema_version (
    version         integer PRIMARY KEY,
    applied_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    description     text NOT NULL
);

INSERT INTO avtobus.schema_version(version, description)
VALUES (1, 'Initial AvtoBus persistence schema')
ON CONFLICT (version) DO NOTHING;

CREATE TABLE IF NOT EXISTS avtobus.outbox_message (
    event_id            uuid PRIMARY KEY,
    event_source        text NOT NULL,
    event_type          text NOT NULL,
    subject             text NULL,
    partition_key       text NULL,
    destination         text NOT NULL,
    content_type        text NOT NULL DEFAULT 'application/cloudevents+json',
    envelope            bytea NOT NULL,
    envelope_sha256     bytea NOT NULL,
    transport_headers   jsonb NOT NULL DEFAULT '{}'::jsonb,
    status              smallint NOT NULL DEFAULT 0,
    available_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempt_count       integer NOT NULL DEFAULT 0,
    max_attempts        integer NOT NULL DEFAULT 20,
    lock_token          uuid NULL,
    locked_by           text NULL,
    locked_until        timestamptz NULL,
    last_error_code     text NULL,
    last_error          text NULL,
    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at             timestamptz NULL,
    CONSTRAINT ck_outbox_status CHECK (status IN (0, 1, 2, 9)),
    CONSTRAINT ck_outbox_attempts CHECK (attempt_count >= 0 AND max_attempts > 0),
    CONSTRAINT ck_outbox_hash CHECK (octet_length(envelope_sha256) = 32),
    CONSTRAINT ck_outbox_headers_object CHECK (jsonb_typeof(transport_headers) = 'object'),
    CONSTRAINT ck_outbox_lease CHECK (
        (status = 1 AND lock_token IS NOT NULL AND locked_by IS NOT NULL AND locked_until IS NOT NULL)
        OR
        (status <> 1 AND lock_token IS NULL AND locked_by IS NULL AND locked_until IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_outbox_pending
    ON avtobus.outbox_message (available_at, event_id)
    WHERE status = 0;

CREATE INDEX IF NOT EXISTS ix_outbox_expired_lease
    ON avtobus.outbox_message (locked_until, event_id)
    WHERE status = 1;

CREATE INDEX IF NOT EXISTS ix_outbox_retention
    ON avtobus.outbox_message (sent_at)
    WHERE status = 2;

CREATE INDEX IF NOT EXISTS ix_outbox_subject
    ON avtobus.outbox_message (event_type, subject)
    WHERE subject IS NOT NULL;

CREATE TABLE IF NOT EXISTS avtobus.inbox_message (
    consumer_name       text NOT NULL,
    event_source        text NOT NULL,
    event_id            uuid NOT NULL,
    event_type          text NOT NULL,
    envelope_sha256     bytea NOT NULL,
    received_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    processed_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_name, event_source, event_id),
    CONSTRAINT ck_inbox_hash CHECK (octet_length(envelope_sha256) = 32)
);

CREATE INDEX IF NOT EXISTS ix_inbox_retention
    ON avtobus.inbox_message (processed_at);

CREATE TABLE IF NOT EXISTS avtobus.scheduled_message (
    schedule_id          uuid PRIMARY KEY,
    event_id             uuid NOT NULL UNIQUE,
    event_source         text NOT NULL,
    event_type           text NOT NULL,
    subject              text NULL,
    partition_key        text NULL,
    destination          text NOT NULL,
    content_type         text NOT NULL DEFAULT 'application/cloudevents+json',
    envelope             bytea NOT NULL,
    envelope_sha256      bytea NOT NULL,
    transport_headers    jsonb NOT NULL DEFAULT '{}'::jsonb,
    due_at               timestamptz NOT NULL,
    cancellation_key     text NULL,
    correlation_id       uuid NULL,
    status               smallint NOT NULL DEFAULT 0,
    attempt_count        integer NOT NULL DEFAULT 0,
    lock_token           uuid NULL,
    locked_by            text NULL,
    locked_until         timestamptz NULL,
    created_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
    enqueued_at          timestamptz NULL,
    cancelled_at         timestamptz NULL,
    last_error           text NULL,
    CONSTRAINT ck_scheduled_status CHECK (status IN (0, 1, 2, 8, 9)),
    CONSTRAINT ck_scheduled_hash CHECK (octet_length(envelope_sha256) = 32),
    CONSTRAINT ck_scheduled_headers CHECK (jsonb_typeof(transport_headers) = 'object'),
    CONSTRAINT ck_scheduled_lease CHECK (
        (status = 1 AND lock_token IS NOT NULL AND locked_by IS NOT NULL AND locked_until IS NOT NULL)
        OR
        (status <> 1 AND lock_token IS NULL AND locked_by IS NULL AND locked_until IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_scheduled_due
    ON avtobus.scheduled_message (due_at, schedule_id)
    WHERE status = 0;

CREATE INDEX IF NOT EXISTS ix_scheduled_expired_lease
    ON avtobus.scheduled_message (locked_until, schedule_id)
    WHERE status = 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_scheduled_cancel_key
    ON avtobus.scheduled_message (cancellation_key)
    WHERE cancellation_key IS NOT NULL AND status IN (0, 1);

CREATE INDEX IF NOT EXISTS ix_scheduled_correlation
    ON avtobus.scheduled_message (correlation_id)
    WHERE correlation_id IS NOT NULL AND status IN (0, 1);

CREATE TABLE IF NOT EXISTS avtobus.process_state (
    process_type         text NOT NULL,
    correlation_id       uuid NOT NULL,
    current_state        text NOT NULL,
    state_data           jsonb NOT NULL,
    version              bigint NOT NULL DEFAULT 1,
    is_completed         boolean NOT NULL DEFAULT false,
    expires_at           timestamptz NULL,
    created_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (process_type, correlation_id),
    CONSTRAINT ck_process_version CHECK (version > 0),
    CONSTRAINT ck_process_state_object CHECK (jsonb_typeof(state_data) = 'object')
);

CREATE INDEX IF NOT EXISTS ix_process_active_state
    ON avtobus.process_state (process_type, current_state, updated_at)
    WHERE is_completed = false;

CREATE INDEX IF NOT EXISTS ix_process_expiration
    ON avtobus.process_state (expires_at)
    WHERE is_completed = false AND expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS avtobus.dead_letter (
    dead_letter_id       uuid PRIMARY KEY,
    event_source         text NOT NULL,
    event_id             uuid NOT NULL,
    event_type           text NOT NULL,
    subject              text NULL,
    destination          text NOT NULL,
    source_kind          smallint NOT NULL,
    consumer_name        text NULL,
    content_type         text NOT NULL,
    envelope             bytea NOT NULL,
    envelope_sha256      bytea NOT NULL,
    transport_headers    jsonb NOT NULL DEFAULT '{}'::jsonb,
    reason_code          text NOT NULL,
    exception_type       text NULL,
    exception_message    text NULL,
    stack_trace          text NULL,
    attempt_count        integer NOT NULL,
    is_security_risk     boolean NOT NULL DEFAULT false,
    status               smallint NOT NULL DEFAULT 0,
    dead_lettered_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    resolved_at          timestamptz NULL,
    resolved_by          text NULL,
    resolution_note      text NULL,
    CONSTRAINT ck_dlq_source_kind CHECK (source_kind IN (0, 1, 2)),
    CONSTRAINT ck_dlq_status CHECK (status IN (0, 1, 2)),
    CONSTRAINT ck_dlq_hash CHECK (octet_length(envelope_sha256) = 32),
    CONSTRAINT ck_dlq_headers CHECK (jsonb_typeof(transport_headers) = 'object')
);

CREATE INDEX IF NOT EXISTS ix_dlq_open
    ON avtobus.dead_letter (dead_lettered_at DESC)
    WHERE status = 0;

CREATE INDEX IF NOT EXISTS ix_dlq_event
    ON avtobus.dead_letter (event_source, event_id);

CREATE INDEX IF NOT EXISTS ix_dlq_security
    ON avtobus.dead_letter (dead_lettered_at DESC)
    WHERE is_security_risk = true AND status = 0;

-- Роль рантайма — идемпотентное создание для дев-окружений;
-- в проде роль создается вне миграции с нужными правами.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'avtobus_runtime') THEN
        CREATE ROLE avtobus_runtime;
    END IF;
END
$$;

REVOKE ALL ON SCHEMA avtobus FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA avtobus FROM PUBLIC;

GRANT USAGE ON SCHEMA avtobus TO avtobus_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA avtobus TO avtobus_runtime;

ALTER DEFAULT PRIVILEGES IN SCHEMA avtobus
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO avtobus_runtime;

COMMIT;
