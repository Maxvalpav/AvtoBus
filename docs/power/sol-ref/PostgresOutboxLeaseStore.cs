using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using AvtoBus.Core;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace AvtoBus.Persistence.Postgres;

public sealed record LeasedOutboxMessage(
    Guid EventId,
    string EventSource,
    string EventType,
    string? Subject,
    string? PartitionKey,
    string Destination,
    string ContentType,
    byte[] Envelope,
    byte[] EnvelopeSha256,
    IReadOnlyDictionary<string, string> TransportHeaders,
    int AttemptCount,
    int MaxAttempts,
    Guid LockToken,
    string LockedBy,
    DateTimeOffset LockedUntil);

public sealed partial class PostgresOutboxLeaseStore
{
    private const string ClaimSql = """
        WITH candidates AS MATERIALIZED (
            SELECT event_id
            FROM avtobus.outbox_message
            WHERE
                (status = 0 AND available_at <= clock_timestamp())
                OR
                (status = 1 AND locked_until <= clock_timestamp())
            ORDER BY available_at, event_id
            LIMIT @batch_size
            FOR UPDATE SKIP LOCKED
        )
        UPDATE avtobus.outbox_message AS o
        SET status = 1,
            attempt_count = o.attempt_count + 1,
            lock_token = @lock_token,
            locked_by = @worker_id,
            locked_until = clock_timestamp() + @lease_duration,
            last_error_code = NULL,
            last_error = NULL
        FROM candidates AS c
        WHERE o.event_id = c.event_id
        RETURNING
            o.event_id,
            o.event_source,
            o.event_type,
            o.subject,
            o.partition_key,
            o.destination,
            o.content_type,
            o.envelope,
            o.envelope_sha256,
            o.transport_headers::text,
            o.attempt_count,
            o.max_attempts,
            o.lock_token,
            o.locked_by,
            o.locked_until;
        """;

    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgresAvtoBusOptions _options;
    private readonly JsonTypeInfo<Dictionary<string, string>> _headersTypeInfo;

    public PostgresOutboxLeaseStore(
        NpgsqlDataSource dataSource,
        IOptions<PostgresAvtoBusOptions> options,
        JsonTypeInfo<Dictionary<string, string>> headersTypeInfo)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _headersTypeInfo = headersTypeInfo;
    }

    public async ValueTask<IReadOnlyList<LeasedOutboxMessage>> ClaimAsync(
        CancellationToken cancellationToken)
    {
        var token = Guid.CreateVersion7();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(ClaimSql, connection, transaction);

        command.Parameters.AddWithValue("batch_size", NpgsqlDbType.Integer, _options.BatchSize);
        command.Parameters.AddWithValue("lock_token", NpgsqlDbType.Uuid, token);
        command.Parameters.AddWithValue("worker_id", NpgsqlDbType.Text, _options.WorkerId);
        command.Parameters.AddWithValue("lease_duration", NpgsqlDbType.Interval, _options.LeaseDuration);

        var result = new List<LeasedOutboxMessage>(_options.BatchSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var headersJson = reader.GetString(9);
            var headers = JsonSerializer.Deserialize(headersJson, _headersTypeInfo)
                ?? new Dictionary<string, string>();

            result.Add(new LeasedOutboxMessage(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetString(5),
                reader.GetString(6),
                reader.GetFieldValue<byte[]>(7),
                reader.GetFieldValue<byte[]>(8),
                headers,
                reader.GetInt32(10),
                reader.GetInt32(11),
                reader.GetGuid(12),
                reader.GetString(13),
                reader.GetFieldValue<DateTimeOffset>(14)));
        }

        await reader.DisposeAsync();
        await transaction.CommitAsync(cancellationToken);
        return result;
    }
}

public sealed partial class PostgresOutboxLeaseStore
{
    private const string MarkSentSql = """
        UPDATE avtobus.outbox_message
        SET status = 2,
            sent_at = clock_timestamp(),
            lock_token = NULL,
            locked_by = NULL,
            locked_until = NULL,
            last_error_code = NULL,
            last_error = NULL
        WHERE event_id = @event_id
          AND status = 1
          AND lock_token = @lock_token
          AND locked_by = @worker_id;
        """;

    public async ValueTask<bool> MarkSentAsync(
        LeasedOutboxMessage item,
        CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(MarkSentSql, connection);
        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Uuid, item.EventId);
        command.Parameters.AddWithValue("lock_token", NpgsqlDbType.Uuid, item.LockToken);
        command.Parameters.AddWithValue("worker_id", NpgsqlDbType.Text, item.LockedBy);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public async ValueTask<bool> ExtendLeaseAsync(
        LeasedOutboxMessage item,
        TimeSpan extension,
        CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE avtobus.outbox_message
            SET locked_until = clock_timestamp() + @extension
            WHERE event_id = @event_id
              AND status = 1
              AND lock_token = @lock_token
              AND locked_by = @worker_id;
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("extension", NpgsqlDbType.Interval, extension);
        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Uuid, item.EventId);
        command.Parameters.AddWithValue("lock_token", NpgsqlDbType.Uuid, item.LockToken);
        command.Parameters.AddWithValue("worker_id", NpgsqlDbType.Text, item.LockedBy);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }
}

public sealed partial class PostgresOutboxLeaseStore
{
    private const string RetrySql = """
        UPDATE avtobus.outbox_message
        SET status = 0,
            available_at = clock_timestamp() + @delay,
            lock_token = NULL,
            locked_by = NULL,
            locked_until = NULL,
            last_error_code = @error_code,
            last_error = @error
        WHERE event_id = @event_id
          AND status = 1
          AND lock_token = @lock_token
          AND locked_by = @worker_id;
        """;

    private const string MoveToDlqSql = """
        WITH dead AS (
            UPDATE avtobus.outbox_message
            SET status = 9,
                lock_token = NULL,
                locked_by = NULL,
                locked_until = NULL,
                last_error_code = @error_code,
                last_error = @error
            WHERE event_id = @event_id
              AND status = 1
              AND lock_token = @lock_token
              AND locked_by = @worker_id
            RETURNING *
        )
        INSERT INTO avtobus.dead_letter (
            dead_letter_id, event_source, event_id, event_type, subject,
            destination, source_kind, consumer_name, content_type,
            envelope, envelope_sha256, transport_headers,
            reason_code, exception_type, exception_message,
            stack_trace, attempt_count, is_security_risk)
        SELECT
            @dead_letter_id, event_source, event_id, event_type, subject,
            destination, 0, NULL, content_type,
            envelope, envelope_sha256, transport_headers,
            @error_code, @exception_type, @error,
            @stack_trace, attempt_count, @is_security_risk
        FROM dead;
        """;

    public async ValueTask HandleFailureAsync(
        LeasedOutboxMessage item,
        string errorCode,
        Exception? exception,
        CancellationToken cancellationToken,
        bool permanent = false,
        bool securityRisk = false)
    {
        var safeMessage = ErrorSanitizer.Message(exception?.Message ?? errorCode);
        var safeStack = ErrorSanitizer.Stack(exception?.StackTrace);
        var isDead = permanent || item.AttemptCount >= item.MaxAttempts;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            isDead ? MoveToDlqSql : RetrySql, connection);

        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Uuid, item.EventId);
        command.Parameters.AddWithValue("lock_token", NpgsqlDbType.Uuid, item.LockToken);
        command.Parameters.AddWithValue("worker_id", NpgsqlDbType.Text, item.LockedBy);
        command.Parameters.AddWithValue("error_code", NpgsqlDbType.Text, errorCode);
        command.Parameters.AddWithValue("error", NpgsqlDbType.Text, safeMessage);

        if (isDead)
        {
            command.Parameters.AddWithValue(
                "dead_letter_id", NpgsqlDbType.Uuid, Guid.CreateVersion7());
            command.Parameters.AddNullable(
                "exception_type", NpgsqlDbType.Text, exception?.GetType().FullName);
            command.Parameters.AddNullable("stack_trace", NpgsqlDbType.Text, safeStack);
            command.Parameters.AddWithValue(
                "is_security_risk", NpgsqlDbType.Boolean, securityRisk);
        }
        else
        {
            var delay = RetryDelay.Calculate(item.AttemptCount, _options.MaxRetryDelay);
            command.Parameters.AddWithValue("delay", NpgsqlDbType.Interval, delay);
        }

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
