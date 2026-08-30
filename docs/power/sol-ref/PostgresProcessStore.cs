using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Npgsql;
using NpgsqlTypes;

namespace AvtoBus.Persistence.Postgres;

public sealed record ProcessState<T>(
    string ProcessType,
    Guid CorrelationId,
    string CurrentState,
    T Data,
    long Version,
    bool IsCompleted,
    DateTimeOffset? ExpiresAt);

public sealed class PostgresProcessStore
{
    public async ValueTask<ProcessState<T>?> LoadAsync<T>(
        AvtoBusDbSession session,
        string processType,
        Guid correlationId,
        JsonTypeInfo<T> jsonTypeInfo,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT current_state, state_data::text, version, is_completed, expires_at
            FROM avtobus.process_state
            WHERE process_type = @process_type
              AND correlation_id = @correlation_id;
            """;

        await using var command = new NpgsqlCommand(
            sql, session.Connection, session.Transaction);
        command.Parameters.AddWithValue("process_type", NpgsqlDbType.Text, processType);
        command.Parameters.AddWithValue("correlation_id", NpgsqlDbType.Uuid, correlationId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken)) return null;
        var data = JsonSerializer.Deserialize(reader.GetString(1), jsonTypeInfo)
            ?? throw new JsonException("Process state data is null.");

        return new ProcessState<T>(
            processType,
            correlationId,
            reader.GetString(0),
            data,
            reader.GetInt64(2),
            reader.GetBoolean(3),
            reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4));
    }

    public async ValueTask CreateAsync<T>(
        AvtoBusDbSession session,
        ProcessState<T> state,
        JsonTypeInfo<T> jsonTypeInfo,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO avtobus.process_state (
                process_type, correlation_id, current_state, state_data,
                version, is_completed, expires_at)
            VALUES (
                @process_type, @correlation_id, @current_state, @state_data::jsonb,
                1, @is_completed, @expires_at);
            """;

        await using var command = BuildWriteCommand(
            sql, session, state, jsonTypeInfo, expectedVersion: null);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask<long> UpdateAsync<T>(
        AvtoBusDbSession session,
        ProcessState<T> state,
        JsonTypeInfo<T> jsonTypeInfo,
        CancellationToken cancellationToken)
    {
        const string sql = """
            UPDATE avtobus.process_state
            SET current_state = @current_state,
                state_data = @state_data::jsonb,
                is_completed = @is_completed,
                expires_at = @expires_at,
                version = version + 1,
                updated_at = clock_timestamp()
            WHERE process_type = @process_type
              AND correlation_id = @correlation_id
              AND version = @expected_version
            RETURNING version;
            """;

        await using var command = BuildWriteCommand(
            sql, session, state, jsonTypeInfo, state.Version);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        if (value is null)
            throw new ProcessConcurrencyException(state.ProcessType, state.CorrelationId);
        return (long)value;
    }

    private static NpgsqlCommand BuildWriteCommand<T>(
        string sql,
        AvtoBusDbSession session,
        ProcessState<T> state,
        JsonTypeInfo<T> jsonTypeInfo,
        long? expectedVersion)
    {
        var command = new NpgsqlCommand(sql, session.Connection, session.Transaction);
        command.Parameters.AddWithValue("process_type", NpgsqlDbType.Text, state.ProcessType);
        command.Parameters.AddWithValue("correlation_id", NpgsqlDbType.Uuid, state.CorrelationId);
        command.Parameters.AddWithValue("current_state", NpgsqlDbType.Text, state.CurrentState);
        command.Parameters.AddWithValue(
            "state_data", NpgsqlDbType.Jsonb, JsonSerializer.Serialize(state.Data, jsonTypeInfo));
        command.Parameters.AddWithValue("is_completed", NpgsqlDbType.Boolean, state.IsCompleted);
        command.Parameters.AddNullable("expires_at", NpgsqlDbType.TimestampTz, state.ExpiresAt);
        if (expectedVersion.HasValue)
            command.Parameters.AddWithValue(
                "expected_version", NpgsqlDbType.Bigint, expectedVersion.Value);
        return command;
    }
}
