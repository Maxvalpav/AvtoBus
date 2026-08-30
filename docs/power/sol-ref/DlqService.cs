using Npgsql;
using NpgsqlTypes;

namespace AvtoBus.Persistence.Postgres;

/// <summary>
/// Сервис управления DLQ: replay и discard согласно разделу 15.2/15.3
/// </summary>
    public sealed class DlqService
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly AvtoBus.Abstractions.ITransportPublisher _publisher;
    private readonly AvtoBus.Core.Security.IMessageSecurity? _security;

    public DlqService(NpgsqlDataSource dataSource, AvtoBus.Abstractions.ITransportPublisher publisher, AvtoBus.Core.Security.IMessageSecurity? security = null)
    {
        _dataSource = dataSource;
        _publisher = publisher;
        _security = security;
    }

    /// <summary>
    /// Replay для Outbox DLQ: вернуть исходную Outbox-строку в Pending (15.2, source_kind=0)
    /// </summary>
    public async ValueTask<bool> ReplayOutboxAsync(Guid deadLetterId, string operatorName, string? note, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        // Пытаемся сначала восстановить существующую outbox запись (status 9 -> 0)
        int restored;
        await using (var cmd1 = new NpgsqlCommand("""
            UPDATE avtobus.outbox_message AS o
            SET status = 0,
                available_at = clock_timestamp(),
                attempt_count = 0,
                lock_token = NULL,
                locked_by = NULL,
                locked_until = NULL,
                last_error_code = NULL,
                last_error = NULL
            FROM avtobus.dead_letter AS d
            WHERE d.dead_letter_id = @dead_letter_id
              AND d.source_kind = 0
              AND d.status = 0
              AND o.event_id = d.event_id;
            """, conn, tx))
        {
            cmd1.Parameters.AddWithValue("dead_letter_id", NpgsqlDbType.Uuid, deadLetterId);
            restored = await cmd1.ExecuteNonQueryAsync(ct);
        }

        // Если записи нет (удалена retention'ом), реинсертим из dead_letter
        if (restored == 0)
        {
            await using var reinsert = new NpgsqlCommand("""
                INSERT INTO avtobus.outbox_message (
                    event_id, event_source, event_type, subject, destination,
                    content_type, envelope, envelope_sha256, transport_headers,
                    status, available_at, attempt_count, max_attempts)
                SELECT
                    d.event_id, d.event_source, d.event_type, d.subject, d.destination,
                    d.content_type, d.envelope, d.envelope_sha256, d.transport_headers,
                    0, clock_timestamp(), 0, 20
                FROM avtobus.dead_letter AS d
                WHERE d.dead_letter_id = @dead_letter_id
                  AND d.source_kind = 0
                  AND d.status = 0
                ON CONFLICT (event_id) DO NOTHING;
                """, conn, tx);
            reinsert.Parameters.AddWithValue("dead_letter_id", NpgsqlDbType.Uuid, deadLetterId);
            var inserted = await reinsert.ExecuteNonQueryAsync(ct);
            if (inserted == 0)
            {
                // Ни update ни insert не сработали — мертвое письмо не найдено или неверный source_kind
                await tx.RollbackAsync(ct);
                return false;
            }
        }

        int affected;
        await using (var cmd2 = new NpgsqlCommand("""
            UPDATE avtobus.dead_letter
            SET status = 1,
                resolved_at = clock_timestamp(),
                resolved_by = @operator,
                resolution_note = @note
            WHERE dead_letter_id = @dead_letter_id
              AND status = 0
              AND source_kind = 0;
            """, conn, tx))
        {
            cmd2.Parameters.AddWithValue("dead_letter_id", NpgsqlDbType.Uuid, deadLetterId);
            cmd2.Parameters.AddWithValue("operator", NpgsqlDbType.Text, operatorName);
            cmd2.Parameters.AddNullable("note", NpgsqlDbType.Text, note);
            affected = await cmd2.ExecuteNonQueryAsync(ct);
        }
        await tx.CommitAsync(ct);
        return affected == 1;
    }

    /// <summary>
    /// Replay для Consumer DLQ: повторная публикация исходных байтов (source_kind=1)
    /// </summary>
    public async ValueTask<bool> ReplayIncomingAsync(Guid deadLetterId, string operatorName, string? note, CancellationToken ct)
    {
        // Load envelope
        const string loadSql = """
            SELECT envelope, content_type, destination, transport_headers::text, is_security_risk
            FROM avtobus.dead_letter
            WHERE dead_letter_id = @id AND status = 0 AND source_kind = 1;
            """;
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        byte[] envelope;
        string contentType;
        string destination;
        string headersJson;
        bool isSecurityRisk;
        await using (var load = new NpgsqlCommand(loadSql, conn))
        {
            load.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, deadLetterId);
            await using var reader = await load.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) return false;
            envelope = reader.GetFieldValue<byte[]>(0);
            contentType = reader.GetString(1);
            destination = reader.GetString(2);
            headersJson = reader.GetString(3);
            isSecurityRisk = reader.GetBoolean(4);
        }

        if (isSecurityRisk)
        {
            // Требуется второе подтверждение - в простейшем случае отклоняем без explicit force
            // Caller должен передать note содержащий "[confirm-security-risk]"
            if (note is null || !note.Contains("[confirm-security-risk]", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Security-risk DLQ requires explicit confirmation note containing [confirm-security-risk]");
        }

        var headers = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(headersJson) ?? new();
        // Переподписываем HMAC если доступен ключ (ротация ключей)
        if (_security is not null)
        {
            var tmpMeta = new AvtoBus.Abstractions.CloudEventMetadata(Guid.Empty, "https://dlq-replay", "dlq.replay", null, null, null, contentType, null, null, null, null, null, null, null);
            var tmpEncoded = new AvtoBus.Core.EncodedCloudEvent(tmpMeta, envelope, System.Security.Cryptography.SHA256.HashData(envelope), contentType, destination, null, headers);
            var reprotected = await _security.ProtectAsync(tmpEncoded, ct);
            headers = new Dictionary<string, string>(reprotected.TransportHeaders);
        }

        var result = await _publisher.PublishAsync(
            new AvtoBus.Abstractions.TransportMessage(envelope, contentType, destination, null, headers), ct);

        if (!result.Confirmed) return false;

        await using var update = new NpgsqlCommand("""
            UPDATE avtobus.dead_letter
            SET status = 1, resolved_at = clock_timestamp(), resolved_by = @op, resolution_note = @note
            WHERE dead_letter_id = @id AND status = 0;
            """, conn);
        update.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, deadLetterId);
        update.Parameters.AddWithValue("op", NpgsqlDbType.Text, operatorName);
        update.Parameters.AddNullable("note", NpgsqlDbType.Text, note);
        return await update.ExecuteNonQueryAsync(ct) == 1;
    }

    public async ValueTask<bool> DiscardAsync(Guid deadLetterId, string operatorName, string? note, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            UPDATE avtobus.dead_letter
            SET status = 2, resolved_at = clock_timestamp(), resolved_by = @op, resolution_note = @note
            WHERE dead_letter_id = @id AND status = 0;
            """, conn);
        cmd.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, deadLetterId);
        cmd.Parameters.AddWithValue("op", NpgsqlDbType.Text, operatorName);
        cmd.Parameters.AddNullable("note", NpgsqlDbType.Text, note);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async ValueTask<IReadOnlyList<DlqEntry>> ListOpenAsync(int limit, int offset, CancellationToken ct)
    {
        const string sql = """
            SELECT dead_letter_id, event_source, event_id, event_type, subject, destination, source_kind,
                   reason_code, exception_type, exception_message, attempt_count, is_security_risk, dead_lettered_at
            FROM avtobus.dead_letter
            WHERE status = 0
            ORDER BY dead_lettered_at DESC
            LIMIT @limit OFFSET @offset;
            """;
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("limit", NpgsqlDbType.Integer, limit);
        cmd.Parameters.AddWithValue("offset", NpgsqlDbType.Integer, offset);
        var list = new List<DlqEntry>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            list.Add(new DlqEntry(
                reader.GetGuid(0), reader.GetString(1), reader.GetGuid(2), reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4), reader.GetString(5), reader.GetInt16(6),
                reader.GetString(7), reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetString(9), reader.GetInt32(10), reader.GetBoolean(11), reader.GetFieldValue<DateTimeOffset>(12)));
        }
        return list;
    }
}

public sealed record DlqEntry(
    Guid DeadLetterId,
    string EventSource,
    Guid EventId,
    string EventType,
    string? Subject,
    string Destination,
    short SourceKind,
    string ReasonCode,
    string? ExceptionType,
    string? ExceptionMessage,
    int AttemptCount,
    bool IsSecurityRisk,
    DateTimeOffset DeadLetteredAt);
