# AvtoBus.Scheduling — Cron, отложенные сообщения, лидер-элекшн

Пакет `AvtoBus.Scheduling` — durable-планировщик поверх БД.

---

## AvtoBus.Scheduling/ScheduledJob.cs

```csharp
namespace AvtoBus.Scheduling;

public sealed class ScheduledJob
{
    public long Id { get; set; }
    public Guid Token { get; set; }
    public string JobKey { get; set; } = "";
    public string? CronExpression { get; set; }
    public string TimeZone { get; set; } = "UTC";
    public DateTime NextFireAt { get; set; }
    public DateTime? LastFireAt { get; set; }
    public string MessageType { get; set; } = "";
    public byte[] EnvelopeBlob { get; set; } = [];
    public bool IsCancelled { get; set; }
    public string? ClaimedBy { get; set; }
    public DateTime? ClaimedAt { get; set; }
    public int MisfireCount { get; set; }
    public string MisfirePolicy { get; set; } = "fire-now";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

public sealed class ScheduledJobModel
{
    public static void Configure(Microsoft.EntityFrameworkCore.ModelBuilder mb)
    {
        mb.Entity<ScheduledJob>(e =>
        {
            e.ToTable("avtobus_scheduled_jobs");
            e.HasKey(x => x.Id);
            e.HasIndex(x => x.Token).IsUnique();
            e.HasIndex(x => new { x.IsCancelled, x.NextFireAt });
            e.HasIndex(x => x.JobKey).IsUnique().HasFilter("\"CronExpression\" IS NOT NULL");
            e.Property(x => x.EnvelopeBlob).HasColumnType("bytea");
        });
    }
}
```

---

## AvtoBus.Scheduling/CronExpression.cs

```csharp
namespace AvtoBus.Scheduling;

/// <summary>
/// Простой парсер cron (5 полей: min hour dom month dow).
/// Для production — Cronos NuGet.
/// </summary>
public sealed class CronExpression
{
    private readonly int[] _minutes;
    private readonly int[] _hours;
    private readonly int[] _daysOfMonth;
    private readonly int[] _months;
    private readonly int[] _daysOfWeek;

    public string Expression { get; }
    public TimeZoneInfo TimeZone { get; }

    public CronExpression(string expression, TimeZoneInfo? tz = null)
    {
        Expression = expression;
        TimeZone = tz ?? TimeZoneInfo.Utc;

        var parts = expression.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 5)
            throw new FormatException("Cron expression must have 5 fields");

        _minutes = ParseField(parts[0], 0, 59);
        _hours = ParseField(parts[1], 0, 23);
        _daysOfMonth = ParseField(parts[2], 1, 31);
        _months = ParseField(parts[3], 1, 12);
        _daysOfWeek = ParseField(parts[4], 0, 6);
    }

    public static CronExpression Parse(string expression, TimeZoneInfo? tz = null) => new(expression, tz);

    public DateTimeOffset? GetNextOccurrence(DateTimeOffset from)
    {
        var local = TimeZoneInfo.ConvertTime(from, TimeZone);
        var candidate = local.AddMinutes(1).AddSeconds(-local.Second).AddMilliseconds(-local.Millisecond);

        for (int i = 0; i < 366 * 24 * 60; i++)
        {
            if (Matches(candidate))
                return TimeZoneInfo.ConvertTime(candidate, TimeZoneInfo.Utc);
            candidate = candidate.AddMinutes(1);
        }
        return null;
    }

    private bool Matches(DateTimeOffset dt) =>
        _minutes.Contains(dt.Minute) &&
        _hours.Contains(dt.Hour) &&
        _daysOfMonth.Contains(dt.Day) &&
        _months.Contains(dt.Month) &&
        _daysOfWeek.Contains((int)dt.DayOfWeek);

    private static int[] ParseField(string field, int min, int max)
    {
        if (field == "*")
            return Enumerable.Range(min, max - min + 1).ToArray();

        var result = new HashSet<int>();
        foreach (var part in field.Split(','))
        {
            if (part.Contains('/'))
            {
                var slash = part.Split('/');
                var step = int.Parse(slash[1]);
                var start = slash[0] == "*" ? min : int.Parse(slash[0]);
                for (int v = start; v <= max; v += step)
                    result.Add(v);
            }
            else if (part.Contains('-'))
            {
                var range = part.Split('-');
                for (int v = int.Parse(range[0]); v <= int.Parse(range[1]); v++)
                    result.Add(v);
            }
            else
            {
                result.Add(int.Parse(part));
            }
        }
        return result.OrderBy(x => x).ToArray();
    }
}
```

---

## AvtoBus.Scheduling/IScheduler.cs

```csharp
namespace AvtoBus.Scheduling;

/// <summary>
/// Планировщик отложенных и cron-сообщений.
/// </summary>
public interface IScheduler
{
    ValueTask<Guid> ScheduleAsync<T>(T message, DateTimeOffset at,
        CancellationToken ct = default) where T : class;

    ValueTask<Guid> ScheduleCronAsync<T>(T message, string cronExpression, string jobKey,
        string? timeZone = null, CancellationToken ct = default) where T : class;

    ValueTask CancelAsync(Guid token, CancellationToken ct = default);

    ValueTask<IReadOnlyList<ScheduledJob>> ListAsync(bool includeCancelled = false,
        CancellationToken ct = default);
}
```

---

## AvtoBus.Scheduling/EfCoreScheduler.cs

```csharp
using AvtoBus.Outbox;
using Microsoft.EntityFrameworkCore;

namespace AvtoBus.Scheduling;

internal sealed class EfCoreScheduler<TDb> : IScheduler where TDb : DbContext
{
    private readonly IDbContextFactory<TDb> _factory;
    private readonly IEnvelopeSerializer _serializer;
    private readonly IRouter _router;
    private readonly ISerializer _payloadSerializer;
    private readonly ITypeResolver _types;
    private readonly TimeProvider _clock;

    public EfCoreScheduler(
        IDbContextFactory<TDb> factory,
        IEnvelopeSerializer serializer,
        IRouter router,
        ISerializer payloadSerializer,
        ITypeResolver types,
        TimeProvider clock)
    {
        _factory = factory;
        _serializer = serializer;
        _router = router;
        _payloadSerializer = payloadSerializer;
        _types = types;
        _clock = clock;
    }

    public async ValueTask<Guid> ScheduleAsync<T>(T message, DateTimeOffset at,
        CancellationToken ct = default) where T : class
    {
        await using var db = await _factory.CreateDbContextAsync(ct);

        var envelope = BuildEnvelope(message, at);
        var token = Guid.NewGuid();

        db.Set<ScheduledJob>().Add(new ScheduledJob
        {
            Token = token,
            JobKey = $"onetime:{token}",
            NextFireAt = at.UtcDateTime,
            MessageType = _types.GetName(typeof(T)),
            EnvelopeBlob = _serializer.Serialize(envelope),
        });

        await db.SaveChangesAsync(ct);
        return token;
    }

    public async ValueTask<Guid> ScheduleCronAsync<T>(T message, string cronExpression, string jobKey,
        string? timeZone = null, CancellationToken ct = default) where T : class
    {
        await using var db = await _factory.CreateDbContextAsync(ct);

        var tz = timeZone is null ? TimeZoneInfo.Utc : TimeZoneInfo.FindSystemTimeZoneById(timeZone);
        var cron = CronExpression.Parse(cronExpression, tz);
        var nextFire = cron.GetNextOccurrence(_clock.GetUtcNow());
        if (nextFire is null)
            throw new InvalidOperationException($"Cron '{cronExpression}' has no future occurrences");

        // Идемпотентно по JobKey
        var existing = await db.Set<ScheduledJob>()
            .FirstOrDefaultAsync(j => j.JobKey == jobKey && !j.IsCancelled, ct);

        if (existing is not null)
        {
            existing.CronExpression = cronExpression;
            existing.TimeZone = tz.Id;
            existing.NextFireAt = nextFire.Value.UtcDateTime;
            await db.SaveChangesAsync(ct);
            return existing.Token;
        }

        var envelope = BuildEnvelope(message, nextFire.Value);
        var token = Guid.NewGuid();

        db.Set<ScheduledJob>().Add(new ScheduledJob
        {
            Token = token,
            JobKey = jobKey,
            CronExpression = cronExpression,
            TimeZone = tz.Id,
            NextFireAt = nextFire.Value.UtcDateTime,
            MessageType = _types.GetName(typeof(T)),
            EnvelopeBlob = _serializer.Serialize(envelope),
        });

        await db.SaveChangesAsync(ct);
        return token;
    }

    public async ValueTask CancelAsync(Guid token, CancellationToken ct = default)
    {
        await using var db = await _factory.CreateDbContextAsync(ct);
        await db.Set<ScheduledJob>()
            .Where(j => j.Token == token)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.IsCancelled, true), ct);
    }

    public async ValueTask<IReadOnlyList<ScheduledJob>> ListAsync(
        bool includeCancelled = false, CancellationToken ct = default)
    {
        await using var db = await _factory.CreateDbContextAsync(ct);
        var query = db.Set<ScheduledJob>().AsQueryable();
        if (!includeCancelled)
            query = query.Where(j => !j.IsCancelled);
        return await query.OrderBy(j => j.NextFireAt).ToListAsync(ct);
    }

    private Envelope BuildEnvelope(object message, DateTimeOffset at)
    {
        var body = _payloadSerializer.Serialize(message);
        return new Envelope
        {
            MessageId = Guid.NewGuid(),
            MessageType = _types.GetName(message.GetType()),
            Body = body,
            SentAt = _clock.GetUtcNow(),
            DeliverAt = at,
        };
    }
}
```

---

## AvtoBus.Scheduling/SchedulerHost.cs

```csharp
using AvtoBus.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Scheduling;

internal sealed class SchedulerHost : BackgroundService
{
    private readonly IServiceScopeFactory _scopes;
    private readonly IBus _bus;
    private readonly IEnvelopeSerializer _serializer;
    private readonly Transport.ITransportSelector _transports;
    private readonly ILeaderElector _leader;
    private readonly SchedulerOptions _options;
    private readonly TimeProvider _clock;
    private readonly ILogger<SchedulerHost> _log;
    private readonly string _hostId = $"{Environment.MachineName}/{Environment.ProcessId}";

    public SchedulerHost(
        IServiceScopeFactory scopes,
        IBus bus,
        IEnvelopeSerializer serializer,
        Transport.ITransportSelector transports,
        ILeaderElector leader,
        SchedulerOptions options,
        TimeProvider clock,
        ILogger<SchedulerHost> log)
    {
        _scopes = scopes;
        _bus = bus;
        _serializer = serializer;
        _transports = transports;
        _leader = leader;
        _options = options;
        _clock = clock;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("SchedulerHost starting, host={Host}", _hostId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Cron-джобы фаерят только на лидере (idempotent enqueue)
                var isLeader = await _leader.TryAcquireAsync(_hostId, stoppingToken);
                await ProcessDueJobs(isLeader, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception ex)
            {
                _log.LogError(ex, "SchedulerHost tick error");
            }

            await Task.Delay(_options.TickInterval, stoppingToken);
        }
    }

    private async Task ProcessDueJobs(bool isLeader, CancellationToken ct)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();
        var now = _clock.GetUtcNow().UtcDateTime;

        // Claim due jobs
        var due = await db.Set<ScheduledJob>()
            .Where(j => !j.IsCancelled && j.NextFireAt <= now)
            .Where(j => j.ClaimedAt == null || j.ClaimedAt < now.AddMinutes(-2))
            .OrderBy(j => j.NextFireAt)
            .Take(_options.BatchSize)
            .ToListAsync(ct);

        if (due.Count == 0) return;

        foreach (var job in due)
        {
            // Cron только с лидера
            if (job.CronExpression is not null && !isLeader) continue;

            try
            {
                var envelope = _serializer.Deserialize(job.EnvelopeBlob);
                var route = new TransportDestination(job.MessageType, DestinationKind.Topic);

                // Публикуем через транспорт (bypass outbox — уже durable)
                var transport = _transports.Default;
                await transport.SendAsync(envelope with { DeliverAt = null }, route, ct);

                job.LastFireAt = now;

                if (job.CronExpression is not null)
                {
                    var tz = TimeZoneInfo.FindSystemTimeZoneById(job.TimeZone);
                    var next = CronExpression.Parse(job.CronExpression, tz)
                        .GetNextOccurrence(_clock.GetUtcNow());
                    if (next.HasValue)
                        job.NextFireAt = next.Value.UtcDateTime;
                    else
                        job.IsCancelled = true;
                }
                else
                {
                    job.IsCancelled = true;   // one-time — done
                }

                job.ClaimedAt = null;
                await db.SaveChangesAsync(ct);
            }
            catch (Exception ex)
            {
                job.MisfireCount++;
                _log.LogWarning(ex, "Job {JobKey} failed", job.JobKey);
                await db.SaveChangesAsync(ct);
            }
        }
    }
}

public sealed class SchedulerOptions
{
    public TimeSpan TickInterval { get; set; } = TimeSpan.FromSeconds(1);
    public int BatchSize { get; set; } = 100;
}
```

---

## AvtoBus.Scheduling/ILeaderElector.cs

```csharp
namespace AvtoBus.Scheduling;

public interface ILeaderElector
{
    ValueTask<bool> TryAcquireAsync(string hostId, CancellationToken ct);
    ValueTask ReleaseAsync(CancellationToken ct);
}

/// <summary>
/// Лидер-элекшн через PostgreSQL advisory lock.
/// Не требует внешнего Zookeeper/etcd.
/// </summary>
public sealed class PostgresAdvisoryLockLeader : ILeaderElector
{
    private readonly IDbContextFactory<DbContext> _factory;
    private readonly long _lockKey;

    public PostgresAdvisoryLockLeader(IDbContextFactory<DbContext> factory, string keyName = "avtobus-scheduler")
    {
        _factory = factory;
        _lockKey = BitConverter.ToInt64(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(keyName)), 0);
    }

    public async ValueTask<bool> TryAcquireAsync(string hostId, CancellationToken ct)
    {
        await using var db = await _factory.CreateDbContextAsync(ct);
        var acquired = await db.Database
            .SqlQueryRaw<bool>("SELECT pg_try_advisory_lock({0})", _lockKey)
            .FirstAsync(ct);
        return acquired;
    }

    public async ValueTask ReleaseAsync(CancellationToken ct)
    {
        await using var db = await _factory.CreateDbContextAsync(ct);
        await db.Database.ExecuteSqlRawAsync("SELECT pg_advisory_unlock({0})", ct, _lockKey);
    }
}
```

---

## AvtoBus.Scheduling/SchedulingRegistration.cs

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus;

public static class SchedulingRegistration
{
    public static BusOptions UseScheduling<TDb>(this BusOptions bus, Action<SchedulerOptions>? configure = null)
        where TDb : DbContext
    {
        var opts = new SchedulerOptions();
        configure?.Invoke(opts);

        bus.Services.AddSingleton(opts);
        bus.Services.AddScoped<IScheduler, EfCoreScheduler<TDb>>();
        bus.Services.AddSingleton<ILeaderElector, PostgresAdvisoryLockLeader>();
        bus.Services.AddHostedService<SchedulerHost>();
        return bus;
    }
}
```

Пример использования:

```csharp
// Program.cs
builder.Services.AddAvtoBus(b => b
    .UseRabbitMq(cs)
    .UseOutbox<AppDb>()
    .UseScheduling<AppDb>());

// В коде
await scheduler.ScheduleAsync(new SendReminder(userId), DateTimeOffset.UtcNow.AddDays(1));
await scheduler.ScheduleCronAsync(new GenerateDailyReport(), "0 6 * * *", "daily-report", "Europe/Moscow");
```
