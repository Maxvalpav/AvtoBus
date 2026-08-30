# 🔧 Реализация: Transactional Outbox + Inbox для EF Core

> **Design draft.** Надёжность Outbox подтверждается только integration-тестами с commit, rollback и crash-сценариями; таких тестов в репозитории пока нет.

Пакет `AvtoBus.Outbox.EfCore`. Работает поверх любой БД, поддерживаемой EF Core; оптимизации — под PostgreSQL и SQL Server.

## 1. Схема БД

```csharp
// AvtoBus.Outbox.EfCore/OutboxMessage.cs
public sealed class OutboxMessage
{
    public long Id { get; set; }                       // BIGSERIAL / IDENTITY
    public Guid MessageId { get; set; }                // Uuid v7 (index)
    public string Destination { get; set; } = "";
    public string Transport { get; set; } = "";
    public string MessageType { get; set; } = "";
    public string? PartitionKey { get; set; }
    public string? TenantId { get; set; }
    public byte[] EnvelopeBlob { get; set; } = [];     // MemoryPack сериализованный Envelope
    public DateTime CreatedAt { get; set; }
    public DateTime? SendAfter { get; set; }
    public DateTime? SentAt { get; set; }
    public DateTime? ClaimedAt { get; set; }
    public string? ClaimedBy { get; set; }             // hostname+pid
    public int Attempt { get; set; }
    public string? LastError { get; set; }
}

public sealed class InboxRecord
{
    public Guid MessageId { get; set; }                // PK
    public string ConsumerId { get; set; } = "";       // PK
    public DateTime ProcessedAt { get; set; }
    public byte[]? Response { get; set; }              // для request-reply идемпотентности
}
```

```csharp
// AvtoBus.Outbox.EfCore/OutboxModelBuilder.cs
public static class OutboxModelBuilder
{
    public static ModelBuilder ConfigureOutbox(this ModelBuilder mb)
    {
        mb.Entity<OutboxMessage>(e =>
        {
            e.ToTable("avtobus_outbox");
            e.HasKey(x => x.Id);
            e.HasIndex(x => x.MessageId).IsUnique();
            e.HasIndex(x => new { x.SentAt, x.SendAfter })
                .HasFilter("\"SentAt\" IS NULL");          // partial index (PG)
            e.Property(x => x.EnvelopeBlob).HasColumnType("bytea");
        });

        mb.Entity<InboxRecord>(e =>
        {
            e.ToTable("avtobus_inbox");
            e.HasKey(x => new { x.MessageId, x.ConsumerId });
            e.HasIndex(x => x.ProcessedAt);              // для чистки TTL
        });

        return mb;
    }
}
```

## 2. Интерфейс Outbox и его EF-реализация

```csharp
public interface IOutbox
{
    ValueTask EnqueueAsync(Envelope env, Route route, CancellationToken ct);
}

internal sealed class EfCoreOutbox<TDbContext> : IOutbox where TDbContext : DbContext
{
    private readonly TDbContext _db;
    private readonly IEnvelopeSerializer _ser;
    private readonly IOutboxSignal _signal;
    private readonly TimeProvider _clock;

    public EfCoreOutbox(TDbContext db, IEnvelopeSerializer ser, IOutboxSignal signal, TimeProvider clock)
        => (_db, _ser, _signal, _clock) = (db, ser, signal, clock);

    public async ValueTask EnqueueAsync(Envelope env, Route route, CancellationToken ct)
    {
        _db.Set<OutboxMessage>().Add(new OutboxMessage
        {
            MessageId    = env.MessageId,
            Destination  = route.Destination.Address,
            Transport    = route.Transport,
            MessageType  = env.MessageType,
            PartitionKey = env.PartitionKey,
            TenantId     = env.TenantId,
            EnvelopeBlob = _ser.Serialize(env),
            CreatedAt    = _clock.GetUtcNow().UtcDateTime,
            SendAfter    = env.DeliverAt?.UtcDateTime,
        });

        // Пробуждаем relay ПОСЛЕ фактического SaveChanges
        _db.SavingChanges += (_, _) => { /* маркер */ };
        _db.SavedChanges  += (_, _) => _signal.Nudge();
    }
}
```

## 3. Interceptor: сигнал релею после коммита

```csharp
public sealed class OutboxSaveChangesInterceptor : SaveChangesInterceptor
{
    private readonly IOutboxSignal _signal;
    public OutboxSaveChangesInterceptor(IOutboxSignal s) => _signal = s;

    public override async ValueTask<int> SavedChangesAsync(
        SaveChangesCompletedEventData eventData, int result, CancellationToken ct = default)
    {
        // Если в этом SaveChanges были OutboxMessage — пнуть релей
        if (eventData.Context is not null &&
            eventData.Context.ChangeTracker.Entries<OutboxMessage>().Any())
        {
            _signal.Nudge();
        }
        return result;
    }
}

// Регистрация:
services.AddDbContext<AppDbContext>((sp, opt) =>
    opt.UseNpgsql(cs).AddInterceptors(sp.GetRequiredService<OutboxSaveChangesInterceptor>()));
```

## 4. Relay: push (Channel) + polling fallback + claim через `SKIP LOCKED`

```csharp
// AvtoBus.Outbox.EfCore/OutboxRelay.cs
internal sealed class OutboxRelay : BackgroundService
{
    private readonly IServiceScopeFactory _scopes;
    private readonly ITransportSelector _transports;
    private readonly IEnvelopeSerializer _ser;
    private readonly IOutboxSignal _signal;
    private readonly OutboxOptions _opt;
    private readonly ILogger<OutboxRelay> _log;
    private readonly string _claimBy = $"{Environment.MachineName}/{Environment.ProcessId}";

    protected override async Task ExecuteAsync(CancellationToken stopping)
    {
        while (!stopping.IsCancellationRequested)
        {
            var pumped = await PumpAsync(stopping);
            if (pumped == 0)
            {
                // Ждём сигнала от SavedChanges или таймер poll-interval
                await _signal.WaitAsync(_opt.PollInterval, stopping);
            }
        }
    }

    private async Task<int> PumpAsync(CancellationToken ct)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();

        // Claim: PostgreSQL — FOR UPDATE SKIP LOCKED в транзакции
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        var claimed = await db.Set<OutboxMessage>()
            .FromSqlInterpolated($"""
                SELECT * FROM avtobus_outbox
                WHERE "SentAt" IS NULL
                  AND ("SendAfter" IS NULL OR "SendAfter" <= {DateTime.UtcNow})
                ORDER BY "Id"
                FOR UPDATE SKIP LOCKED
                LIMIT {_opt.BatchSize}
            """)
            .ToListAsync(ct);

        if (claimed.Count == 0) { await tx.CommitAsync(ct); return 0; }

        foreach (var m in claimed) { m.ClaimedAt = DateTime.UtcNow; m.ClaimedBy = _claimBy; }
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);

        // Отправка вне транзакции — параллельно, с per-partition упорядочиванием
        var sent = new List<long>(claimed.Count);
        var failed = new List<(long id, string err)>();

        await Parallel.ForEachAsync(
            claimed.GroupBy(m => m.PartitionKey ?? m.MessageId.ToString()),
            new ParallelOptions { MaxDegreeOfParallelism = _opt.Parallelism, CancellationToken = ct },
            async (group, token) =>
            {
                foreach (var m in group) // внутри партиции — строго по порядку
                {
                    try
                    {
                        var env = _ser.Deserialize(m.EnvelopeBlob);
                        var transport = _transports.For(m.Transport);
                        await transport.SendAsync(env,
                            new TransportDestination(m.Destination, DestinationKind.Queue), token);
                        lock (sent) sent.Add(m.Id);
                    }
                    catch (Exception ex)
                    {
                        _log.LogWarning(ex, "Outbox send failed for {MessageId}", m.MessageId);
                        lock (failed) failed.Add((m.Id, ex.Message));
                        break; // сохраняем порядок партиции — стоп при первой ошибке
                    }
                }
            });

        // Отметить отправленные одним UPDATE
        if (sent.Count > 0)
        {
            await db.Set<OutboxMessage>()
                .Where(o => sent.Contains(o.Id))
                .ExecuteUpdateAsync(s => s
                    .SetProperty(o => o.SentAt, DateTime.UtcNow)
                    .SetProperty(o => o.ClaimedAt, (DateTime?)null), ct);
        }

        if (failed.Count > 0)
        {
            foreach (var (id, err) in failed)
                await db.Set<OutboxMessage>()
                    .Where(o => o.Id == id)
                    .ExecuteUpdateAsync(s => s
                        .SetProperty(o => o.Attempt, o => o.Attempt + 1)
                        .SetProperty(o => o.LastError, err)
                        .SetProperty(o => o.ClaimedAt, (DateTime?)null), ct);
        }

        return claimed.Count;
    }
}

public sealed class OutboxOptions
{
    public int BatchSize { get; set; } = 200;
    public int Parallelism { get; set; } = 8;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan CleanupAfter { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan StaleClaim { get; set; } = TimeSpan.FromMinutes(2);
}
```

## 5. In-process сигнал (Channel-based)

```csharp
public interface IOutboxSignal
{
    void Nudge();
    Task WaitAsync(TimeSpan timeout, CancellationToken ct);
}

internal sealed class ChannelOutboxSignal : IOutboxSignal
{
    private readonly Channel<byte> _ch =
        Channel.CreateBounded<byte>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    public void Nudge() => _ch.Writer.TryWrite(0);

    public async Task WaitAsync(TimeSpan timeout, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try { await _ch.Reader.ReadAsync(cts.Token); } catch (OperationCanceledException) { }
    }
}
```

## 6. Inbox-дедупликация (middleware)

```csharp
public sealed class InboxDedupMiddleware : IBusMiddleware
{
    private readonly IServiceScopeFactory _scopes;
    private readonly InboxOptions _opt;

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();
        var consumerId = ctx.Envelope.Headers.GetValueOrDefault("consumer") ?? "default";

        // Bloom-фильтр (быстрый негатив) — не показан ради краткости
        try
        {
            db.Set<InboxRecord>().Add(new InboxRecord
            {
                MessageId = ctx.Envelope.MessageId,
                ConsumerId = consumerId,
                ProcessedAt = DateTime.UtcNow
            });
            await db.SaveChangesAsync(ctx.CancellationToken);
        }
        catch (DbUpdateException ex) when (IsUniqueViolation(ex))
        {
            // Дубликат — тихо ack без обработки
            BusMetrics.InboxDeduped.Add(1);
            return;
        }

        await next(ctx);
        // (в реальной реализации SaveChanges inbox выполняется в общей транзакции с хендлером)
    }

    private static bool IsUniqueViolation(DbUpdateException ex) =>
        ex.InnerException is Npgsql.PostgresException { SqlState: "23505" }
        || (ex.InnerException?.Message.Contains("UNIQUE") ?? false);
}
```

## 7. Migrations и очистка

```csharp
// AvtoBus.Outbox.EfCore/Migrations/20260101_InitAvtoBusOutbox.cs
public partial class InitAvtoBusOutbox : Migration
{
    protected override void Up(MigrationBuilder mb) => mb.Sql("""
        CREATE TABLE avtobus_outbox (
            "Id" bigserial PRIMARY KEY,
            "MessageId" uuid NOT NULL UNIQUE,
            "Destination" text NOT NULL,
            "Transport" text NOT NULL,
            "MessageType" text NOT NULL,
            "PartitionKey" text NULL,
            "TenantId" text NULL,
            "EnvelopeBlob" bytea NOT NULL,
            "CreatedAt" timestamptz NOT NULL,
            "SendAfter" timestamptz NULL,
            "SentAt" timestamptz NULL,
            "ClaimedAt" timestamptz NULL,
            "ClaimedBy" text NULL,
            "Attempt" int NOT NULL DEFAULT 0,
            "LastError" text NULL
        );
        CREATE INDEX ix_outbox_pending ON avtobus_outbox("SendAfter") WHERE "SentAt" IS NULL;

        CREATE TABLE avtobus_inbox (
            "MessageId" uuid NOT NULL,
            "ConsumerId" text NOT NULL,
            "ProcessedAt" timestamptz NOT NULL,
            "Response" bytea NULL,
            PRIMARY KEY ("MessageId","ConsumerId")
        );
        CREATE INDEX ix_inbox_processed ON avtobus_inbox("ProcessedAt");
    """);
}

internal sealed class OutboxCleanup : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            await using var scope = _scopes.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<DbContext>();
            var cutoff = DateTime.UtcNow - _opt.CleanupAfter;

            await db.Set<OutboxMessage>().Where(o => o.SentAt != null && o.SentAt < cutoff)
                .ExecuteDeleteAsync(stop);
            await db.Set<InboxRecord>().Where(i => i.ProcessedAt < cutoff)
                .ExecuteDeleteAsync(stop);
            await Task.Delay(TimeSpan.FromMinutes(15), stop);
        }
    }
}
```

## 8. Регистрация одной строкой

```csharp
public static class OutboxRegistration
{
    public static BusOptions UseOutbox<TDb>(this BusOptions bus, Action<OutboxOptions>? configure = null)
        where TDb : DbContext
    {
        var opt = new OutboxOptions();
        configure?.Invoke(opt);

        bus.Services.AddSingleton(opt);
        bus.Services.AddSingleton<IOutboxSignal, ChannelOutboxSignal>();
        bus.Services.AddSingleton<OutboxSaveChangesInterceptor>();
        bus.Services.AddScoped<IOutbox, EfCoreOutbox<TDb>>();
        bus.Services.AddSingleton<IEnvelopeSerializer, MemoryPackEnvelopeSerializer>();
        bus.Services.AddHostedService<OutboxRelay>();
        bus.Services.AddHostedService<OutboxCleanup>();
        return bus;
    }
}
```

**Итог:** пользователь пишет `bus.UseOutbox<AppDbContext>()` — получает transactional outbox с push+polling, `SKIP LOCKED`, партиционным порядком, авточисткой и inbox-дедупликацией.
