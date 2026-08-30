using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Npgsql;
using NpgsqlTypes;
using AvtoBus.Core;

namespace AvtoBus.Persistence.Postgres;

public sealed class PostgresOutboxWriter
{
    private const string InsertSql = """
        INSERT INTO avtobus.outbox_message (
            event_id, event_source, event_type, subject, partition_key,
            destination, content_type, envelope, envelope_sha256,
            transport_headers, available_at, max_attempts)
        VALUES (
            @event_id, @event_source, @event_type, @subject, @partition_key,
            @destination, @content_type, @envelope, @envelope_sha256,
            @transport_headers::jsonb, @available_at, @max_attempts)
        ON CONFLICT (event_id) DO NOTHING;
        """;

    private readonly IOutboxSignal _signal;
    private readonly JsonTypeInfo<Dictionary<string, string>> _headersJsonTypeInfo;

    public PostgresOutboxWriter(
        IOutboxSignal signal,
        JsonTypeInfo<Dictionary<string, string>> headersJsonTypeInfo)
    {
        _signal = signal;
        _headersJsonTypeInfo = headersJsonTypeInfo;
    }

    public async ValueTask EnqueueAsync(
        AvtoBusDbSession session,
        EncodedCloudEvent item,
        DateTimeOffset? availableAt = null,
        int maxAttempts = 20,
        CancellationToken cancellationToken = default)
    {
        await using var command = new NpgsqlCommand(
            InsertSql, session.Connection, session.Transaction);

        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Uuid, item.Metadata.Id);
        command.Parameters.AddWithValue("event_source", NpgsqlDbType.Text, item.Metadata.Source);
        command.Parameters.AddWithValue("event_type", NpgsqlDbType.Text, item.Metadata.Type);
        command.Parameters.AddNullable("subject", NpgsqlDbType.Text, item.Metadata.Subject);
        command.Parameters.AddNullable("partition_key", NpgsqlDbType.Text, item.PartitionKey);
        command.Parameters.AddWithValue("destination", NpgsqlDbType.Text, item.Destination);
        command.Parameters.AddWithValue("content_type", NpgsqlDbType.Text, item.ContentType);
        command.Parameters.AddWithValue("envelope", NpgsqlDbType.Bytea, item.Envelope);
        command.Parameters.AddWithValue("envelope_sha256", NpgsqlDbType.Bytea, item.Sha256);

        var mutableHeaders = new Dictionary<string, string>(
            item.TransportHeaders, StringComparer.OrdinalIgnoreCase);
        var headersJson = JsonSerializer.Serialize(mutableHeaders, _headersJsonTypeInfo);
        command.Parameters.AddWithValue("transport_headers", NpgsqlDbType.Jsonb, headersJson);
        command.Parameters.AddWithValue(
            "available_at", NpgsqlDbType.TimestampTz, availableAt ?? DateTimeOffset.UtcNow);
        command.Parameters.AddWithValue("max_attempts", NpgsqlDbType.Integer, maxAttempts);

        var affected = await command.ExecuteNonQueryAsync(cancellationToken);
        if (affected == 0)
            throw new DuplicateOutboxEventException(item.Metadata.Id);
    }

    public void NotifyCommitted() => _signal.Pulse();
}
