using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Npgsql;
using NpgsqlTypes;
using AvtoBus.Abstractions;
using AvtoBus.Core;

namespace AvtoBus.Persistence.Postgres;

public sealed class PostgresDlqStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly JsonTypeInfo<Dictionary<string, string>> _headersTypeInfo;

    public PostgresDlqStore(
        NpgsqlDataSource dataSource,
        JsonTypeInfo<Dictionary<string, string>> headersTypeInfo)
    {
        _dataSource = dataSource;
        _headersTypeInfo = headersTypeInfo;
    }

    public async ValueTask StoreIncomingAsync(
        IReceivedTransportMessage received,
        string reasonCode,
        Exception exception,
        bool securityRisk,
        CancellationToken cancellationToken)
    {
        CloudEventMetadata metadata;
        try
        {
            metadata = CloudEventHeaderReader.Read(received.Body.Span);
        }
        catch
        {
            metadata = new CloudEventMetadata(
                Guid.CreateVersion7(),
                "urn:avtobus:invalid-message",
                "invalid.cloudevent",
                DateTimeOffset.UtcNow,
                null, null, received.ContentType,
                null, null, null, null, null, null, null);
        }

        const string sql = """
            INSERT INTO avtobus.dead_letter (
                dead_letter_id, event_source, event_id, event_type, subject,
                destination, source_kind, consumer_name, content_type,
                envelope, envelope_sha256, transport_headers,
                reason_code, exception_type, exception_message,
                stack_trace, attempt_count, is_security_risk)
            VALUES (
                @dead_letter_id, @event_source, @event_id, @event_type, @subject,
                @destination, 1, @consumer_name, @content_type,
                @envelope, @hash, @headers::jsonb,
                @reason_code, @exception_type, @exception_message,
                @stack_trace, @attempt_count, @is_security_risk);
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("dead_letter_id", NpgsqlDbType.Uuid, Guid.CreateVersion7());
        command.Parameters.AddWithValue("event_source", NpgsqlDbType.Text, metadata.Source);
        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Uuid, metadata.Id);
        command.Parameters.AddWithValue("event_type", NpgsqlDbType.Text, metadata.Type);
        command.Parameters.AddNullable("subject", NpgsqlDbType.Text, metadata.Subject);
        command.Parameters.AddWithValue("destination", NpgsqlDbType.Text, received.SourceQueue);
        command.Parameters.AddNullable("consumer_name", NpgsqlDbType.Text, null);
        command.Parameters.AddWithValue("content_type", NpgsqlDbType.Text, received.ContentType);
        command.Parameters.AddWithValue("envelope", NpgsqlDbType.Bytea, received.Body.ToArray());
        command.Parameters.AddWithValue("hash", NpgsqlDbType.Bytea, SHA256.HashData(received.Body.Span));
        command.Parameters.AddWithValue(
            "headers",
            NpgsqlDbType.Jsonb,
            JsonSerializer.Serialize(
                new Dictionary<string, string>(received.Headers),
                _headersTypeInfo));
        command.Parameters.AddWithValue("reason_code", NpgsqlDbType.Text, reasonCode);
        command.Parameters.AddWithValue("exception_type", NpgsqlDbType.Text, exception.GetType().FullName!);
        command.Parameters.AddWithValue(
            "exception_message", NpgsqlDbType.Text, ErrorSanitizer.Message(exception.Message));
        command.Parameters.AddNullable(
            "stack_trace", NpgsqlDbType.Text, ErrorSanitizer.Stack(exception.StackTrace));
        command.Parameters.AddWithValue("attempt_count", NpgsqlDbType.Integer, DeliveryCount(received.Headers));
        command.Parameters.AddWithValue("is_security_risk", NpgsqlDbType.Boolean, securityRisk);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static int DeliveryCount(IReadOnlyDictionary<string, string> headers) =>
        headers.TryGetValue("avtobus-delivery-count", out var raw)
        && int.TryParse(raw, out var count)
            ? count
            : 1;
}
