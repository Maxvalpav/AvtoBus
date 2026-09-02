using AvtoBus.Migrations;

namespace AvtoBus.EventSourcing.Migrations;

/// <summary>GDPR: subject_id колонка + индекс для O(log N) отчёта (идея 287). v2 дополняет avtobus-events.</summary>
public sealed class GdprSubjectIndexMigration : ISchemaMigration
{
    public string ModuleName => "avtobus-events";
    public int Version => 2;
    public string Sql => """
        ALTER TABLE avtobus_events ADD COLUMN IF NOT EXISTS subject_id TEXT;
        CREATE INDEX IF NOT EXISTS ix_avtobus_events_subject_id ON avtobus_events(subject_id) WHERE subject_id IS NOT NULL;
        """;
}
