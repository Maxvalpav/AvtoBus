using AvtoBus.Migrations;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// Схема transactional outbox (док 15, §1): таблицы <c>avtobus_outbox</c> и <c>avtobus_inbox</c>.
/// Равнозначна EF-маппингу из <see cref="OutboxModelBuilder.ConfigureOutbox"/> и нужна приложениям,
/// которые не используют EF Migrations/EnsureCreated, а поднимают схему SQL-скриптом при старте (B12).
/// DDL идемпотентный и совпадает по именам колонок с EF-моделью (BIGSERIAL/UUID/BYTEA — PostgreSQL).
/// </summary>
public sealed class OutboxSchemaMigration : ISchemaMigration
{
    public const string Module = "avtobus-outbox";

    public const int CurrentVersion = 1;

    public string ModuleName => Module;

    public int Version => CurrentVersion;

    public string Sql => """
        CREATE TABLE IF NOT EXISTS avtobus_outbox (
            "Id"            BIGSERIAL PRIMARY KEY,
            "MessageId"     UUID NOT NULL,
            "Destination"   TEXT NOT NULL,
            "Transport"     TEXT NOT NULL,
            "MessageType"   TEXT NOT NULL,
            "PartitionKey"  TEXT NULL,
            "TenantId"      TEXT NULL,
            "EnvelopeBlob"  BYTEA NOT NULL,
            "CreatedAt"     TIMESTAMPTZ NOT NULL,
            "SendAfter"     TIMESTAMPTZ NULL,
            "SentAt"        TIMESTAMPTZ NULL,
            "ClaimedAt"     TIMESTAMPTZ NULL,
            "ClaimedBy"     TEXT NULL,
            "Attempt"       INT NOT NULL,
            "LastError"     TEXT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS "IX_avtobus_outbox_MessageId"
            ON avtobus_outbox ("MessageId");

        CREATE INDEX IF NOT EXISTS "IX_avtobus_outbox_SentAt_SendAfter"
            ON avtobus_outbox ("SentAt", "SendAfter")
            WHERE "SentAt" IS NULL;

        CREATE TABLE IF NOT EXISTS avtobus_inbox (
            "MessageId"   UUID NOT NULL,
            "ConsumerId"  TEXT NOT NULL,
            "ProcessedAt" TIMESTAMPTZ NOT NULL,
            "Response"    BYTEA NULL,
            PRIMARY KEY ("MessageId", "ConsumerId")
        );

        CREATE INDEX IF NOT EXISTS "IX_avtobus_inbox_ProcessedAt"
            ON avtobus_inbox ("ProcessedAt");
        """;
}
