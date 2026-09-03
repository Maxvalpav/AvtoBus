using AvtoBus.Migrations;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// Миграция v2 модуля avtobus-outbox: колонка вида назначения + покрывающий индекс claim.
/// Без Kind топики через outbox уезжали в одноимённую очередь вместо fan-out.
/// </summary>
public sealed class OutboxSchemaMigrationV2 : ISchemaMigration
{
    public string ModuleName => OutboxSchemaMigration.Module;

    public int Version => 2;

    public string Sql => """
        ALTER TABLE avtobus_outbox ADD COLUMN IF NOT EXISTS "Kind" INT NOT NULL DEFAULT 0;

        DROP INDEX IF EXISTS "IX_avtobus_outbox_SentAt_SendAfter";
        CREATE INDEX IF NOT EXISTS "IX_avtobus_outbox_claim"
            ON avtobus_outbox ("SentAt", "SendAfter", "ClaimedAt", "Id")
            WHERE "SentAt" IS NULL;
        """;
}
