using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Npgsql;
using NpgsqlTypes;
using AvtoBus.Abstractions;
using AvtoBus.Core;

namespace AvtoBus.Persistence.Postgres;

public sealed class PostgresScheduledWriter
{
    private const string InsertSql = """
        INSERT INTO avtobus.scheduled_message (
            schedule_id, event_id, event_source, event_type, subject,
            partition_key, destination, content_type, envelope,
            envelope_sha256, transport_headers, due_at,
            cancellation_key, correlation_id)
        VALUES (
            @schedule_id, @event_id, @event_source, @event_type, @subject,
            @partition_key, @destination, @content_type, @envelope,
            @envelope_sha256, @transport_headers::jsonb, @due_at,
            @cancellation_key, @correlation_id);
        """;

    private readonly JsonTypeInfo<Dictionary<string, string>> _headersTypeInfo;
    private readonly IScheduledSignal _signal;

    public PostgresScheduledWriter(
        JsonTypeInfo<Dictionary<string, string>> headersTypeInfo,
        IScheduledSignal signal)
    {
        _headersTypeInfo = headersTypeInfo;
        _signal = signal;
    }

    public async ValueTask<Guid> ScheduleAsync(
        AvtoBusDbSession session,
        EncodedCloudEvent item,
        DateTimeOffset dueAt,
        ScheduleOptions options,
        CancellationToken cancellationToken)
    {
        var scheduleId = Guid.CreateVersion7();
        await using var command = new NpgsqlCommand(
            InsertSql, session.Connection, session.Transaction);

        command.Parameters.AddWithValue("schedule_id", NpgsqlDbType.Uuid, scheduleId);
        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Uuid, item.Metadata.Id);
        command.Parameters.AddWithValue("event_source", NpgsqlDbType.Text, item.Metadata.Source);
        command.Parameters.AddWithValue("event_type", NpgsqlDbType.Text, item.Metadata.Type);
        command.Parameters.AddNullable("subject", NpgsqlDbType.Text, item.Metadata.Subject);
        command.Parameters.AddNullable("partition_key", NpgsqlDbType.Text, item.PartitionKey);
        command.Parameters.AddWithValue("destination", NpgsqlDbType.Text, item.Destination);
        command.Parameters.AddWithValue("content_type", NpgsqlDbType.Text, item.ContentType);
        command.Parameters.AddWithValue("envelope", NpgsqlDbType.Bytea, item.Envelope);
        command.Parameters.AddWithValue("envelope_sha256", NpgsqlDbType.Bytea, item.Sha256);
        command.Parameters.AddWithValue(
            "transport_headers",
            NpgsqlDbType.Jsonb,
            JsonSerializer.Serialize(
                new Dictionary<string, string>(item.TransportHeaders),
                _headersTypeInfo));
        command.Parameters.AddWithValue("due_at", NpgsqlDbType.TimestampTz, dueAt);
        command.Parameters.AddNullable(
            "cancellation_key", NpgsqlDbType.Text, options.CancellationKey);
        command.Parameters.AddNullable(
            "correlation_id", NpgsqlDbType.Uuid, options.ProcessCorrelationId);

        await command.ExecuteNonQueryAsync(cancellationToken);
        return scheduleId;
    }

    public async ValueTask<bool> CancelAsync(
        AvtoBusDbSession session,
        string cancellationKey,
        CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE avtobus.scheduled_message
            SET status = 8,
                cancelled_at = clock_timestamp(),
                lock_token = NULL,
                locked_by = NULL,
                locked_until = NULL
            WHERE cancellation_key = @key
              AND status IN (0, 1);
            """;

        await using var command = new NpgsqlCommand(
            sql, session.Connection, session.Transaction);
        command.Parameters.AddWithValue("key", NpgsqlDbType.Text, cancellationKey);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public void NotifyCommitted() => _signal.Pulse();
}
