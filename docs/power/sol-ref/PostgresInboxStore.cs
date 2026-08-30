using Npgsql;
using NpgsqlTypes;
using AvtoBus.Abstractions;
using AvtoBus.Core;

namespace AvtoBus.Persistence.Postgres;

public sealed class PostgresInboxStore
{
    private const string InsertSql = """
        INSERT INTO avtobus.inbox_message (
            consumer_name, event_source, event_id, event_type, envelope_sha256)
        VALUES (
            @consumer_name, @event_source, @event_id, @event_type, @envelope_sha256)
        ON CONFLICT (consumer_name, event_source, event_id) DO NOTHING;
        """;

    public async ValueTask<bool> TryAcquireAsync(
        AvtoBusDbSession session,
        string consumerName,
        CloudEventMetadata metadata,
        byte[] envelopeSha256,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            InsertSql, session.Connection, session.Transaction);
        command.Parameters.AddWithValue("consumer_name", NpgsqlDbType.Text, consumerName);
        command.Parameters.AddWithValue("event_source", NpgsqlDbType.Text, metadata.Source);
        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Uuid, metadata.Id);
        command.Parameters.AddWithValue("event_type", NpgsqlDbType.Text, metadata.Type);
        command.Parameters.AddWithValue("envelope_sha256", NpgsqlDbType.Bytea, envelopeSha256);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }
}
