# AvtoBus.Scheduling — Cron, отложенные сообщения, leader election

> **Code sketch / unverified.** Cron, time zones, misfire и leader election требуют deterministic и integration tests. Канонический статус: [`../FINAL.md`](../FINAL.md).

---

## AvtoBus.Scheduling/ScheduledMessage.cs

```csharp
namespace AvtoBus.Scheduling;

/// <summary>
/// Отложенное сообщение в durable-хранилище.
/// </summary>
public sealed class ScheduledMessage
{
    public long Id { get; set; }
    public Guid Token { get; set; }
    public string MessageType { get; set; } = "";
    public byte[] EnvelopeBlob { get; set; } = [];
    public string Destination { get; set; } = "";
    public string Transport { get; set; } = "";
    public DateTime DeliverAt { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? DeliveredAt { get; set; }
    public DateTime? CancelledAt { get; set; }
    public string? UniqueKey { get; set; }
    public string? TenantId { get; set; }
    public int Attempt { get; set; }
}

/// <summary>
/// Cron-расписание.
/// </summary>
public sealed class CronSchedule
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public string CronExpression { get; set; } = "";
    public string TimeZoneId { get; set; } = "UTC";
    public string MessageType { get; set; } = "";
    public byte[] PayloadBlob { get; set; } = [];
    public DateTime? LastFiredAt { get; set; }
    public DateTime NextFireAt { get; set; }
    public bool IsEnabled { get; set; } = true;
    public MisfirePolicy Misfire { get; set; } = MisfirePolicy.FireLatest;
}

public enum MisfirePolicy
{
    /// <summary>Отработать все пропущенные срабатывания.</summary>
    FireAll,
    /// <summary>Отработать только последнее пропущенное.</summary>
    FireLatest,
    /// <summary>Пропустить и ждать следующего по расписанию.</summary>
    Skip,
}
```

---

## AvtoBus.Scheduling/IScheduleStore.cs

```csharp
namespace AvtoBus.Scheduling;

public interface IScheduleStore
{
    ValueTask<Guid> ScheduleAsync(ScheduledMessage message, CancellationToken ct = default);
    ValueTask CancelAsync(Guid token, CancellationToken ct = default);
    ValueTask<IReadOnlyList<ScheduledMessage>> ClaimDueAsync(
        DateTime now, int batchSize, string claimedBy, CancellationToken ct = default);
    ValueTask MarkDeliveredAsync(IReadOnlyList<long> ids, CancellationToken ct = default);

    // Cron
    ValueTask UpsertCronAsync(CronSchedule schedule, CancellationToken ct = default);
    ValueTask<IReadOnlyList<CronSchedule>> ClaimDueCronAsync(
        DateTime now, string claimedBy, CancellationToken ct = default);
    ValueTask UpdateCronAfterFireAsync(long id, DateTime firedAt, DateTime nextFireAt, CancellationToken ct = default);
    ValueTask<IReadOnlyList<CronSchedule>> ListCronAsync(CancellationToken ct = default);
}
```

---

## AvtoBus.Scheduling/Postgres/PostgresScheduleStore.cs

```csharp
using Npgsql;
using NpgsqlTypes;

namespace AvtoBus.Scheduling.Postgres;

public sealed class PostgresScheduleStore : IScheduleStore
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgresScheduleStore(NpgsqlDataSource dataSource) => _dataSource = dataSource;

    public async ValueTask<Guid> ScheduleAsync(ScheduledMessage message, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);

        // Unique-джобы: игнорируем дубликат в окне (идея Oban)
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO avtobus_scheduled
                (token, message_type, envelope, destination, transport,
                 deliver_at, created_at, unique_key, tenant_id)
            VALUES (@token, @type, @envelope, @dest, @transport,
                    @deliver_at, now(), @unique_key, @tenant)
            ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND delivered_at IS NULL
            DO NOTHING
            RETURNING token
            """, conn);

        cmd.Parameters.AddWithValue("token", message.Token);
        cmd.Parameters.AddWithValue("type", message.MessageType);
        cmd.Parameters.AddWithValue("envelope", NpgsqlDbType.Bytea, message.EnvelopeBlob);
        cmd.Parameters.AddWithValue("dest", message.Destination);
        cmd.Parameters.AddWithValue("transport", message.Transport);
        cmd.Parameters.AddWithValue("deliver_at", message.DeliverAt);
        cmd.Parameters.AddWithValue("unique_key", (object?)message.UniqueKey ?? DBNull.Value);
        cmd.Parameters.AddWithValue("tenant", (object?)message.TenantId ?? DBNull.Value);

        var result = await cmd.ExecuteScalarAsync(ct);
        return result is Guid g ? g : message.Token;
    }

    public async ValueTask CancelAsync(Guid token, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            UPDATE avtobus_scheduled SET cancelled_at = now()
            WHERE token = @token AND delivered_at IS NULL
            """, conn);
        cmd.Parameters.AddWithValue("token", token);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask<IReadOnlyList<ScheduledMessage>> ClaimDueAsync(
        DateTime now, int batchSize, string claimedBy, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);

        // Атомарный claim через CTE + SKIP LOCKED
        await using var cmd = new NpgsqlCommand("""
            WITH due AS (
                SELECT id FROM avtobus_scheduled
                WHERE deliver_at <= @now
                  AND delivered_at IS NULL
                  AND cancelled_at IS NULL
                ORDER BY deliver_at
                LIMIT @limit
                FOR UPDATE SKIP LOCKED
            )
            UPDATE avtobus_scheduled s
            SET claimed_at = now(), claimed_by = @by
            FROM due
            WHERE s.id = due.id
            RETURNING s.id, s.token, s.message_type, s.envelope, s.destination,
                      s.transport, s.deliver_at, s.created_at, s.unique_key, s.tenant_id, s.attempt
            """, conn);

        cmd.Parameters.AddWithValue("now", now);
        cmd.Parameters.AddWithValue("limit", batchSize);
        cmd.Parameters.AddWithValue("by", claimedBy);

        var list = new List<ScheduledMessage>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            list.Add(new ScheduledMessage
            {
                Id = reader.GetInt64(0),
                Token = reader.GetGuid(1),
                MessageType = reader.GetString(2),
                EnvelopeBlob = (byte[])reader[3],
                Destination = reader.GetString(4),
                Transport = reader.GetString(5),
                DeliverAt = reader.GetDateTime(6),
                CreatedAt = reader.GetDateTime(7),
                UniqueKey = reader.IsDBNull(8) ? null : reader.GetString(8),
                TenantId = reader.IsDBNull(9) ? null : reader.GetString(9),
                Attempt = reader.GetInt32(10),
            });
        }
        return list;
    }

    public async ValueTask MarkDeliveredAsync(IReadOnlyList<long> ids, CancellationToken ct = default)
    {
        if (ids.Count == 0) return;
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "UPDATE avtobus_scheduled SET delivered_at = now() WHERE id = ANY(@ids)", conn);
        cmd.Parameters.AddWithValue("ids", ids.ToArray());
        await cmd.ExecuteNonQueryAsync(ct);
    }

    // ── Cron ──

    public async ValueTask UpsertCronAsync(CronSchedule schedule, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            INSERT INTO avtobus_cron
                (name, cron_expression, timezone_id, message_type, payload, next_fire_at, is_enabled, misfire)
            VALUES (@name, @cron, @tz, @type, @payload, @next, @enabled, @misfire)
            ON CONFLICT (name) DO UPDATE
            SET cron_expression = @cron, timezone_id = @tz, message_type = @type,
                payload = @payload, next_fire_at = @next, is_enabled = @enabled, misfire = @misfire
            """, conn);
        cmd.Parameters.AddWithValue("name", schedule.Name);
        cmd.Parameters.AddWithValue("cron", schedule.CronExpression);
        cmd.Parameters.AddWithValue("tz", schedule.TimeZoneId);
        cmd.Parameters.AddWithValue("type", schedule.MessageType);
        cmd.Parameters.AddWithValue("payload", NpgsqlDbType.Bytea, schedule.PayloadBlob);
        cmd.Parameters.AddWithValue("next", schedule.NextFireAt);
        cmd.Parameters.AddWithValue("enabled", schedule.IsEnabled);
        cmd.Parameters.AddWithValue("misfire", (int)schedule.Misfire);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask<IReadOnlyList<CronSchedule>> ClaimDueCronAsync(
        DateTime now, string claimedBy, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT id, name, cron_expression, timezone_id, message_type, payload,
                   last_fired_at, next_fire_at, is_enabled, misfire
            FROM avtobus_cron
            WHERE is_enabled AND next_fire_at <= @now
            FOR UPDATE SKIP LOCKED
            """, conn);
        cmd.Parameters.AddWithValue("now", now);

        var list = new List<CronSchedule>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            list.Add(new CronSchedule
            {
                Id = reader.GetInt64(0),
                Name = reader.GetString(1),
                CronExpression = reader.GetString(2),
                TimeZoneId = reader.GetString(3),
                MessageType = reader.GetString(4),
                PayloadBlob = (byte[])reader[5],
                LastFiredAt = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                NextFireAt = reader.GetDateTime(7),
                IsEnabled = reader.GetBoolean(8),
                Misfire = (MisfirePolicy)reader.GetInt32(9),
            });
        }
        return list;
    }

    public async ValueTask UpdateCronAfterFireAsync(
        long id, DateTime firedAt, DateTime nextFireAt, CancellationToken ct = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "UPDATE avtobus_cron SET last_fired_at = @fired, next_fire_at = @next WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("fired", firedAt);
        cmd.Parameters.AddWithValue("next", nextFireAt);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask<IReadOnlyList<CronSchedule>> ListCronAsync(CancellationToken ct = default)
        => await ClaimDueCronAsync(DateTime.MaxValue, "list", ct);
}
```

---

## AvtoBus.Scheduling/Schema.sql

```sql
CREATE TABLE IF NOT EXISTS avtobus_scheduled (
    id            BIGSERIAL PRIMARY KEY,
    token         UUID        NOT NULL UNIQUE,
    message_type  TEXT        NOT NULL,
    envelope      BYTEA       NOT NULL,
    destination   TEXT        NOT NULL,
    transport     TEXT        NOT NULL,
    deliver_at    TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at  TIMESTAMPTZ NULL,
    cancelled_at  TIMESTAMPTZ NULL,
    claimed_at    TIMESTAMPTZ NULL,
    claimed_by    TEXT        NULL,
    unique_key    TEXT        NULL,
    tenant_id     TEXT        NULL,
    attempt       INT         NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ix_scheduled_due
    ON avtobus_scheduled (deliver_at)
    WHERE delivered_at IS NULL AND cancelled_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_scheduled_unique
    ON avtobus_scheduled (unique_key)
    WHERE unique_key IS NOT NULL AND delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS avtobus_cron (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT        NOT NULL UNIQUE,
    cron_expression TEXT        NOT NULL,
    timezone_id     TEXT        NOT NULL DEFAULT 'UTC',
    message_type    TEXT        NOT NULL,
    payload         BYTEA       NOT NULL,
    last_fired_at   TIMESTAMPTZ NULL,
    next_fire_at    TIMESTAMPTZ NOT NULL,
    is_enabled      BOOLEAN     NOT NULL DEFAULT TRUE,
    misfire         INT         NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS ix_cron_next ON avtobus_cron (next_fire_at) WHERE is_enabled;
```

---

## AvtoBus.Scheduling/CronExpression.cs

```csharp
namespace AvtoBus.Scheduling;

/// <summary>
/// Парсер cron-выражений (5 или 6 полей: [sec] min hour day month dow).
/// Поддержка: * , - / и имена месяцев/дней.
/// </summary>
public sealed class CronExpression
{
    private readonly bool[] _seconds = new bool[60];
    private readonly bool[] _minutes = new bool[60];
    private readonly bool[] _hours = new bool[24];
    private readonly bool[] _daysOfMonth = new bool[32];
    private readonly bool[] _months = new bool[13];
    private readonly bool[] _daysOfWeek = new bool[7];

    public string Expression { get; }

    private CronExpression(string expression) => Expression = expression;

    public static CronExpression Parse(string expression)
    {
        var cron = new CronExpression(expression);
        var parts = expression.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length is not (5 or 6))
            throw new FormatException($"Cron must have 5 or 6 fields, got {parts.Length}: '{expression}'");

        var offset = parts.Length == 6 ? 1 : 0;

        if (offset == 1)
            ParseField(parts[0], cron._seconds, 0, 59);
        else
            cron._seconds[0] = true;  // 5-полевой: срабатываем на нулевой секунде

        ParseField(parts[offset + 0], cron._minutes, 0, 59);
        ParseField(parts[offset + 1], cron._hours, 0, 23);
        ParseField(parts[offset + 2], cron._daysOfMonth, 1, 31);
        ParseField(NormalizeMonths(parts[offset + 3]), cron._months, 1, 12);
        ParseField(NormalizeDows(parts[offset + 4]), cron._daysOfWeek, 0, 6);

        return cron;
    }

    private static void ParseField(string field, bool[] target, int min, int max)
    {
        foreach (var part in field.Split(','))
        {
            var step = 1;
            var range = part;

            if (part.Contains('/'))
            {
                var split = part.Split('/');
                range = split[0];
                step = int.Parse(split[1]);
            }

            int from, to;
            if (range == "*")
            {
                from = min; to = max;
            }
            else if (range.Contains('-'))
            {
                var split = range.Split('-');
                from = int.Parse(split[0]);
                to = int.Parse(split[1]);
            }
            else
            {
                from = to = int.Parse(range);
            }

            for (var i = from; i <= to; i += step)
                if (i >= min && i <= max)
                    target[i] = true;
        }
    }

    private static string NormalizeMonths(string f) => f.ToUpperInvariant()
        .Replace("JAN", "1").Replace("FEB", "2").Replace("MAR", "3").Replace("APR", "4")
        .Replace("MAY", "5").Replace("JUN", "6").Replace("JUL", "7").Replace("AUG", "8")
        .Replace("SEP", "9").Replace("OCT", "10").Replace("NOV", "11").Replace("DEC", "12");

    private static string NormalizeDows(string f) => f.ToUpperInvariant()
        .Replace("SUN", "0").Replace("MON", "1").Replace("TUE", "2").Replace("WED", "3")
        .Replace("THU", "4").Replace("FRI", "5").Replace("SAT", "6").Replace("7", "0");

    /// <summary>
    /// Следующее срабатывание после указанного момента (в заданной TZ).
    /// </summary>
    public DateTimeOffset? GetNextOccurrence(DateTimeOffset after, TimeZoneInfo tz)
    {
        var local = TimeZoneInfo.ConvertTime(after, tz).DateTime.AddSeconds(1);
        var limit = local.AddYears(2);   // защита от бесконечного цикла

        // Секундная точность — перебор с шагом 1с, но с быстрыми скачками
        while (local < limit)
        {
            if (!_months[local.Month]) { local = new DateTime(local.Year, local.Month, 1).AddMonths(1); continue; }
            if (!_daysOfMonth[local.Day] || !_daysOfWeek[(int)local.DayOfWeek])
            { local = local.Date.AddDays(1); continue; }
            if (!_hours[local.Hour]) { local = local.Date.AddHours(local.Hour + 1); continue; }
            if (!_minutes[local.Minute]) { local = local.AddMinutes(1).AddSeconds(-local.Second); continue; }
            if (!_seconds[local.Second]) { local = local.AddSeconds(1); continue; }

            return new DateTimeOffset(local, tz.GetUtcOffset(local)).ToUniversalTime();
        }

        return null;
    }

    /// <summary>Предпросмотр: следующие N срабатываний (для дашборда).</summary>
    public IEnumerable<DateTimeOffset> Preview(DateTimeOffset from, TimeZoneInfo tz, int count)
    {
        var current = from;
        for (var i = 0; i < count; i++)
        {
            var next = GetNextOccurrence(current, tz);
            if (next is null) yield break;
            yield return next.Value;
            current = next.Value;
        }
    }
}
```

---

## AvtoBus.Scheduling/SchedulerService.cs

```csharp
using Microsoft.Extensions.Hosting;

namespace AvtoBus.Scheduling;

/// <summary>
/// Фоновый сервис: доставляет отложенные сообщения и запускает cron-джобы.
/// Cron защищён leader election — только одна реплика в кластере фаерит.
/// </summary>
public sealed class SchedulerService : BackgroundService
{
    private readonly IScheduleStore _store;
    private readonly ITransportSelector _transports;
    private readonly IEnvelopeSerializer _envelopes;
    private readonly ILeaderElection _leader;
    private readonly SchedulerOptions _options;
    private readonly TimeProvider _clock;
    private readonly ILogger<SchedulerService> _log;
    private readonly string _instanceId = $"{Environment.MachineName}/{Environment.ProcessId}";

    public SchedulerService(
        IScheduleStore store,
        ITransportSelector transports,
        IEnvelopeSerializer envelopes,
        ILeaderElection leader,
        SchedulerOptions options,
        TimeProvider clock,
        ILogger<SchedulerService> log)
    {
        _store = store;
        _transports = transports;
        _envelopes = envelopes;
        _leader = leader;
        _options = options;
        _clock = clock;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var delayedTask = RunDelayedLoopAsync(ct);
        var cronTask = RunCronLoopAsync(ct);
        await Task.WhenAll(delayedTask, cronTask);
    }

    // ── Отложенные сообщения (могут обрабатывать все реплики) ──

    private async Task RunDelayedLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var now = _clock.GetUtcNow().UtcDateTime;
                var due = await _store.ClaimDueAsync(now, _options.BatchSize, _instanceId, ct);

                if (due.Count == 0)
                {
                    await Task.Delay(_options.PollInterval, ct);
                    continue;
                }

                var delivered = new List<long>(due.Count);

                foreach (var msg in due)
                {
                    try
                    {
                        var envelope = _envelopes.Deserialize(msg.EnvelopeBlob);
                        var transport = _transports.For(msg.Transport);
                        await transport.SendAsync(
                            envelope with { DeliverAt = null },
                            new TransportDestination(msg.Destination, DestinationKind.Queue),
                            ct);
                        delivered.Add(msg.Id);
                    }
                    catch (Exception ex)
                    {
                        _log.LogError(ex, "Failed to deliver scheduled message {Token}", msg.Token);
                    }
                }

                await _store.MarkDeliveredAsync(delivered, ct);
                _log.LogDebug("Delivered {Count} scheduled messages", delivered.Count);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _log.LogError(ex, "Scheduler delayed-loop error");
                await Task.Delay(_options.ErrorDelay, ct);
            }
        }
    }

    // ── Cron (только лидер) ──

    private async Task RunCronLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!await _leader.TryAcquireAsync("avtobus-cron", _options.LeaderLease, ct))
                {
                    await Task.Delay(_options.LeaderRetryInterval, ct);
                    continue;
                }

                _log.LogInformation("Acquired cron leadership: {Instance}", _instanceId);

                while (!ct.IsCancellationRequested && await _leader.RenewAsync("avtobus-cron", _options.LeaderLease, ct))
                {
                    await FireDueCronAsync(ct);
                    await Task.Delay(_options.CronPollInterval, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _log.LogError(ex, "Scheduler cron-loop error");
                await Task.Delay(_options.ErrorDelay, ct);
            }
        }

        await _leader.ReleaseAsync("avtobus-cron", CancellationToken.None);
    }

    private async Task FireDueCronAsync(CancellationToken ct)
    {
        var now = _clock.GetUtcNow();
        var due = await _store.ClaimDueCronAsync(now.UtcDateTime, _instanceId, ct);

        foreach (var schedule in due)
        {
            try
            {
                var cron = CronExpression.Parse(schedule.CronExpression);
                var tz = TimeZoneInfo.FindSystemTimeZoneById(schedule.TimeZoneId);

                // Misfire-политика
                var fireCount = schedule.Misfire switch
                {
                    MisfirePolicy.FireAll => CountMissed(cron, schedule, now, tz),
                    MisfirePolicy.FireLatest => 1,
                    MisfirePolicy.Skip => 0,
                    _ => 1
                };

                for (var i = 0; i < fireCount; i++)
                {
                    var envelope = _envelopes.Deserialize(schedule.PayloadBlob);
                    var transport = _transports.Default;
                    await transport.SendAsync(
                        envelope with { MessageId = Guid.NewGuid(), SentAt = now },
                        new TransportDestination(schedule.MessageType, DestinationKind.Topic),
                        ct);
                }

                var next = cron.GetNextOccurrence(now, tz)
                           ?? now.AddYears(1);

                await _store.UpdateCronAfterFireAsync(
                    schedule.Id, now.UtcDateTime, next.UtcDateTime, ct);

                _log.LogInformation("Cron '{Name}' fired {Count}x, next at {Next}",
                    schedule.Name, fireCount, next);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Cron '{Name}' failed", schedule.Name);
            }
        }
    }

    private static int CountMissed(CronExpression cron, CronSchedule schedule, DateTimeOffset now, TimeZoneInfo tz)
    {
        if (schedule.LastFiredAt is not { } last) return 1;
        var count = 0;
        var cursor = new DateTimeOffset(last, TimeSpan.Zero);
        while (count < 100)
        {
            var next = cron.GetNextOccurrence(cursor, tz);
            if (next is null || next > now) break;
            count++;
            cursor = next.Value;
        }
        return Math.Max(1, count);
    }
}

public sealed class SchedulerOptions
{
    public int BatchSize { get; set; } = 100;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan CronPollInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ErrorDelay { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan LeaderLease { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan LeaderRetryInterval { get; set; } = TimeSpan.FromSeconds(10);
}
```

---

## AvtoBus.Scheduling/ILeaderElection.cs

```csharp
namespace AvtoBus.Scheduling;

/// <summary>
/// Выбор лидера для single-instance задач в кластере.
/// </summary>
public interface ILeaderElection
{
    ValueTask<bool> TryAcquireAsync(string resource, TimeSpan lease, CancellationToken ct = default);
    ValueTask<bool> RenewAsync(string resource, TimeSpan lease, CancellationToken ct = default);
    ValueTask ReleaseAsync(string resource, CancellationToken ct = default);
    bool IsLeader(string resource);
}

/// <summary>
/// Leader election через PostgreSQL advisory locks — без ZooKeeper/etcd.
/// </summary>
public sealed class PostgresLeaderElection : ILeaderElection, IAsyncDisposable
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly Dictionary<string, NpgsqlConnection> _held = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    public PostgresLeaderElection(NpgsqlDataSource dataSource) => _dataSource = dataSource;

    public async ValueTask<bool> TryAcquireAsync(string resource, TimeSpan lease, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            if (_held.ContainsKey(resource)) return true;

            var conn = await _dataSource.OpenConnectionAsync(ct);
            await using var cmd = new NpgsqlCommand("SELECT pg_try_advisory_lock(@key)", conn);
            cmd.Parameters.AddWithValue("key", (long)resource.GetHashCode());

            var acquired = (bool)(await cmd.ExecuteScalarAsync(ct))!;
            if (acquired)
            {
                _held[resource] = conn;   // держим соединение — держим лок
                return true;
            }

            await conn.DisposeAsync();
            return false;
        }
        finally { _lock.Release(); }
    }

    public async ValueTask<bool> RenewAsync(string resource, TimeSpan lease, CancellationToken ct = default)
    {
        // Advisory lock живёт пока живёт соединение — просто проверяем соединение
        if (!_held.TryGetValue(resource, out var conn)) return false;
        try
        {
            await using var cmd = new NpgsqlCommand("SELECT 1", conn);
            await cmd.ExecuteScalarAsync(ct);
            return true;
        }
        catch
        {
            _held.Remove(resource);
            return false;
        }
    }

    public async ValueTask ReleaseAsync(string resource, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            if (!_held.Remove(resource, out var conn)) return;
            await using (conn)
            await using (var cmd = new NpgsqlCommand("SELECT pg_advisory_unlock(@key)", conn))
            {
                cmd.Parameters.AddWithValue("key", (long)resource.GetHashCode());
                await cmd.ExecuteScalarAsync(ct);
            }
        }
        finally { _lock.Release(); }
    }

    public bool IsLeader(string resource) => _held.ContainsKey(resource);

    public async ValueTask DisposeAsync()
    {
        foreach (var key in _held.Keys.ToList())
            await ReleaseAsync(key);
        _lock.Dispose();
    }
}
```

---

## AvtoBus.Scheduling/Registration.cs

```csharp
namespace AvtoBus;

public static class SchedulingRegistration
{
    public static BusOptions UseScheduling(
        this BusOptions bus,
        string connectionString,
        Action<SchedulerOptions>? configure = null)
    {
        var options = new SchedulerOptions();
        configure?.Invoke(options);

        bus.Services.AddSingleton(options);
        bus.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
        bus.Services.AddSingleton<IScheduleStore, PostgresScheduleStore>();
        bus.Services.AddSingleton<ILeaderElection, PostgresLeaderElection>();
        bus.Services.AddSingleton<ICronRegistry, CronRegistry>();
        bus.Services.AddHostedService<SchedulerService>();
        bus.Services.AddHostedService<CronBootstrapper>();

        return bus;
    }
}

/// <summary>
/// Регистрация cron-джобов в коде.
/// </summary>
public interface ICronRegistry
{
    void Add<TMessage>(string name, string cronExpression, TMessage payload,
        string timeZoneId = "UTC", MisfirePolicy misfire = MisfirePolicy.FireLatest)
        where TMessage : class;

    IReadOnlyList<CronRegistration> Registrations { get; }
}

public sealed record CronRegistration(
    string Name,
    string CronExpression,
    string TimeZoneId,
    object Payload,
    Type PayloadType,
    MisfirePolicy Misfire);

internal sealed class CronRegistry : ICronRegistry
{
    private readonly List<CronRegistration> _registrations = new();
    public IReadOnlyList<CronRegistration> Registrations => _registrations;

    public void Add<TMessage>(string name, string cronExpression, TMessage payload,
        string timeZoneId = "UTC", MisfirePolicy misfire = MisfirePolicy.FireLatest)
        where TMessage : class
    {
        CronExpression.Parse(cronExpression);   // валидация на старте
        _registrations.Add(new CronRegistration(
            name, cronExpression, timeZoneId, payload, typeof(TMessage), misfire));
    }
}

/// <summary>
/// Синхронизирует cron-регистрации из кода в БД при старте.
/// </summary>
internal sealed class CronBootstrapper(
    ICronRegistry registry,
    IScheduleStore store,
    ISerializer serializer,
    TimeProvider clock,
    ILogger<CronBootstrapper> log) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        foreach (var reg in registry.Registrations)
        {
            var cron = CronExpression.Parse(reg.CronExpression);
            var tz = TimeZoneInfo.FindSystemTimeZoneById(reg.TimeZoneId);
            var next = cron.GetNextOccurrence(clock.GetUtcNow(), tz) ?? clock.GetUtcNow().AddYears(1);

            await store.UpsertCronAsync(new CronSchedule
            {
                Name = reg.Name,
                CronExpression = reg.CronExpression,
                TimeZoneId = reg.TimeZoneId,
                MessageType = MessageTypeNaming.For(reg.PayloadType),
                PayloadBlob = serializer.Serialize(reg.Payload).ToArray(),
                NextFireAt = next.UtcDateTime,
                Misfire = reg.Misfire,
                IsEnabled = true,
            }, ct);

            log.LogInformation("Cron '{Name}' registered: {Expr} ({Tz}), next {Next}",
                reg.Name, reg.CronExpression, reg.TimeZoneId, next);
        }
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
```

Использование:

```csharp
builder.Services.AddAvtoBus(bus =>
{
    bus.UseRabbitMq(cs);
    bus.UseScheduling(pgConnectionString);
});

// Регистрация cron-джобов
var cron = app.Services.GetRequiredService<ICronRegistry>();
cron.Add("daily-report", "0 6 * * *", new GenerateDailyReport(), "Europe/Moscow");
cron.Add("cleanup", "*/15 * * * *", new CleanupTempFiles());
```
