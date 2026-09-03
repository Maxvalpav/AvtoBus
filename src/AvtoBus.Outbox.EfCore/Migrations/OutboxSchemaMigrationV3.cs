using AvtoBus.Migrations;

namespace AvtoBus.Outbox.EfCore;

/// <summary>
/// Миграция v3 модуля avtobus-outbox: таблица партиционных лиз.
/// Без неё два relay-инстанса забирают строки одного PartitionKey конкурентно
/// и порядок per key нарушается (аудит A1). DDL идемпотентный.
/// </summary>
public sealed class OutboxSchemaMigrationV3 : ISchemaMigration
{
    public string ModuleName => OutboxSchemaMigration.Module;

    public int Version => 3;

    public string Sql => """
        CREATE TABLE IF NOT EXISTS avtobus_outbox_leases (
            "PartitionKey" TEXT NOT NULL PRIMARY KEY,
            "Owner"        TEXT NOT NULL,
            "ExpiresAt"    TIMESTAMPTZ NOT NULL
        );
        """;
}
