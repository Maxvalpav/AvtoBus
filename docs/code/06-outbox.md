# AvtoBus.Outbox.EfCore — Transactional Outbox

> **Code sketch / unverified.** Транзакционная граница не считается доказанной без PostgreSQL integration tests. Канонический статус: [`../FINAL.md`](../FINAL.md).

Полная реализация Transactional Outbox + Inbox для EF Core.

---

## AvtoBus.Outbox.EfCore/OutboxMessage.cs

```csharp
namespace AvtoBus.Outbox;

/// <summary>
/// Запись в таблице outbox. Помечается при WriteChanges, отправляется relay'ем.
/// </summary>
public sealed class OutboxMessage
{
    public long Id { get; set; }
    public Guid MessageId { get; set; }
    public string Destination { get; set; } = "";
    public string Transport { get; set; } = "";
    public string MessageType { get; set; } = "";
    public string? PartitionKey { get; set; }
    public string? TenantId { get; set; }
    public byte[] EnvelopeBlob { get; set; } = [];
    public DateTime CreatedAt { get; set; }
    public DateTime? SendAfter { get; set; }
    public DateTime? SentAt { get; set; }
    public DateTime? ClaimedAt { get; set; }
    public string? ClaimedBy { get; set; }
    public int Attempt { get; set; }
    public string? LastError { get; set; }
}

/// <summary>
/// Запись в таблице inbox — для дедупликации.
/// </summary>
public sealed class InboxRecord
{
    public Guid MessageId { get; set; }
    public string ConsumerId { get; set; } = "";
    public DateTime ProcessedAt { get; set; }
    public byte[]? Response { get; set; }
}
```

---

## AvtoBus.Outbox.EfCore/OutboxModelBuilder.cs

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace AvtoBus.Outbox;

public static class OutboxModelCreating
{
    public static ModelBuilder ConfigureOutbox(this ModelBuilder builder)
    {
        builder.Entity<OutboxMessage>(e =>
        {
            e.ToTable("avtobus_outbox");
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).UseIdentityAlwaysColumn();
            e.HasIndex(x => x.MessageId).IsUnique();
            e.HasIndex(x => new { x.SentAt, x.SendAfter });
            e.Property(x => x.EnvelopeBlob).HasColumnType("bytea").IsRequired();
            e.Property(x => x.MessageType).HasMaxLength(500);
            e.Property(x => x.Destination).HasMaxLength(500);
            e.Property(x => x.Transport).HasMaxLength(100);
            e.Property(x => x.ClaimedBy).HasMaxLength(200);
            e.Property(x => x.LastError).HasMaxLength(2000);
        });

        builder.Entity<InboxRecord>(e =>
        {
            e.ToTable("avtobus_inbox");
            e.HasKey(x => new { x.MessageId, x.ConsumerId });
            e.HasIndex(x => x.ProcessedAt);
            e.Property(x => x.ConsumerId).HasMaxLength(200);
        });

        return builder;
    }
}
```

---

## AvtoBus.Outbox.EfCore/IOutbox.cs

```csharp
namespace AvtoBus.Outbox;

/// <summary>
/// Запись в outbox (внутри транзакции с бизнес-данными).
/// </summary>
public interface IOutbox
{
    ValueTask EnqueueAsync(Envelope envelope, Route route, CancellationToken ct = default);
}
```

---

## AvtoBus.Outbox.EfCore/EfCoreOutbox.cs

```csharp
using AvtoBus.Transport;
using Microsoft.EntityFrameworkCore;

namespace AvtoBus.Outbox;

internal sealed class EfCoreOutbox<TDbContext> : IOutbox where TDbContext : DbContext
{
    private readonly IDbContextFactory<TDbContext> _factory;
    private readonly IEnvelopeSerializer _serializer;
    private readonly IOutboxSignal _signal;
    private readonly TimeProvider _clock;
    private readonly OutboxOptions _options;

    public EfCoreOutbox(
        IDbContextFactory<TDbContext> factory,
        IEnvelopeSerializer serializer,
        IOutboxSignal signal,
        TimeProvider clock,
        OutboxOptions options)
    {
        _factory = factory;
        _serializer = serializer;
        _signal = signal;
        _clock = clock;
        _options = options;
    }

    public async ValueTask EnqueueAsync(Envelope envelope, Route route, CancellationToken ct = default)
    {
        await using var db = await _factory.CreateDbContextAsync(ct);

        var message = new OutboxMessage
        {
            MessageId    = envelope.MessageId,
            MessageType  = envelope.MessageType,
            Destination  = route.Destination.Address,
            Transport    = route.Transport,
            PartitionKey = envelope.PartitionKey,
            TenantId     = envelope.TenantId,
            EnvelopeBlob = _serializer.Serialize(envelope),
            CreatedAt    = _clock.GetUtcNow().UtcDateTime,
            SendAfter    = envelope.DeliverAt?.UtcDateTime,
        };

        db.Set<OutboxMessage>().Add(message);
        await db.SaveChangesAsync(ct);

        // Пробудить relay
        _signal.Nudge();
    }
}
```

---

## AvtoBus.Outbox.EfCore/IOutboxSignal.cs

```csharp
using System.Threading.Channels;

namespace AvtoBus.Outbox;

public interface IOutboxSignal
{
    void Nudge();
    Task WaitAsync(TimeSpan timeout, CancellationToken ct);
}

internal sealed class ChannelOutboxSignal : IOutboxSignal
{
    private readonly Channel<byte> _channel = Channel.CreateBounded<byte>(
        new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
        });

    public void Nudge()
    {
        _channel.Writer.TryWrite(0);
    }

    public async Task WaitAsync(TimeSpan timeout, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try
        {
            await _channel.Reader.WaitToReadAsync(cts.Token);
            // Сброс — читаем чтобы канал снова был пуст
            while (_channel.Reader.TryRead(out _)) { }
        }
        catch (OperationCanceledException)
        {
            // Timeout — нормально
        }
    }
}
```

---

## AvtoBus.Outbox.EfCore/OutboxRelay.cs

```csharp
using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Outbox;

/// <summary>
/// BackgroundService: отправляет сообщения из outbox в брокер.
/// Два механизма: push (Channel-signal) + polling fallback.
/// Использует SKIP LOCKED для конкурентных реплик.
/// </summary>
internal sealed class OutboxRelay : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IOutboxSignal _signal;
    private readonly Transport.ITransportSelector _transports;
    private readonly IEnvelopeSerializer _serializer;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxRelay> _log;
    private readonly string _claimBy = $"{Environment.MachineName}/{Environment.ProcessId}";

    public OutboxRelay(
        IServiceScopeFactory scopeFactory,
        IOutboxSignal signal,
        Transport.ITransportSelector transports,
        IEnvelopeSerializer serializer,
        OutboxOptions options,
        ILogger<OutboxRelay> log)
    {
        _scopeFactory = scopeFactory;
        _signal = signal;
        _transports = transports;
        _serializer = serializer;
        _options = options;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("OutboxRelay started. BatchSize={Batch}, Parallelism={Parallel}",
            _options.BatchSize, _options.Parallelism);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var processed = await ProcessBatchAsync(stoppingToken);

                if (processed == 0)
                {
                    // Ничего не отправили — ждём сигнал или poll-interval
                    await _signal.WaitAsync(_options.PollInterval, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "OutboxRelay error");
                await Task.Delay(_options.PollInterval, stoppingToken);
            }
        }

        _log.LogInformation("OutboxRelay stopped.");
    }

    private async Task<int> ProcessBatchAsync(CancellationToken ct)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<DbContext>();

        // 1. Claim batch: FOR UPDATE SKIP LOCKED
        var messages = await db.Set<OutboxMessage>()
            .Where(o => o.SentAt == null && (o.SendAfter == null || o.SendAfter <= DateTime.UtcNow))
            .OrderBy(o => o.Id)
            .Take(_options.BatchSize)
            .ToListAsync(ct);

        if (messages.Count == 0)
            return 0;

        // 2. Mark claimed
        var ids = messages.Select(m => m.Id).ToList();
        var now = DateTime.UtcNow;
        await db.Set<OutboxMessage>()
            .Where(o => ids.Contains(o.Id))
            .ExecuteUpdateAsync(s => s
                .SetProperty(o => o.ClaimedAt, now)
                .SetProperty(o => o.ClaimedBy, _claimBy), ct);

        _log.LogDebug("Claimed {Count} outbox messages", messages.Count);

        // 3. Send grouped by partition key (preserves order within partition)
        var sent = new List<long>();
        var failed = new List<(long Id, string Error)>();

        var groups = messages.GroupBy(m => m.PartitionKey ?? m.MessageId.ToString());
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = _options.Parallelism,
            CancellationToken = ct
        };

        await Parallel.ForEachAsync(groups, parallelOptions, async (group, token) =>
        {
            foreach (var msg in group)
            {
                try
                {
                    var envelope = _serializer.Deserialize(msg.EnvelopeBlob);
                    var transport = _transports.For(msg.Transport);
                    var dest = new TransportDestination(msg.Destination, DestinationKind.Queue);
                    await transport.SendAsync(envelope, dest, token);

                    lock (sent) sent.Add(msg.Id);
                }
                catch (Exception ex)
                {
                    lock (failed) failed.Add((msg.Id, ex.Message));
                    _log.LogWarning(ex, "Failed to send outbox message {MessageId}", msg.MessageId);
                    break; // Стоп по партиции — сохраняем порядок
                }
            }
        });

        // 4. Mark sent / update errors
        if (sent.Count > 0)
        {
            var sentAt = DateTime.UtcNow;
            await db.Set<OutboxMessage>()
                .Where(o => sent.Contains(o.Id))
                .ExecuteUpdateAsync(s => s
                    .SetProperty(o => o.SentAt, sentAt)
                    .SetProperty(o => o.ClaimedAt, (DateTime?)null), ct);
        }

        foreach (var (id, error) in failed)
        {
            await db.Set<OutboxMessage>()
                .Where(o => o.Id == id)
                .ExecuteUpdateAsync(s => s
                    .SetProperty(o => o.Attempt, o => o.Attempt + 1)
                    .SetProperty(o => o.LastError, error)
                    .SetProperty(o => o.ClaimedAt, (DateTime?)null), ct);
        }

        _log.LogInformation("OutboxRelay: sent={Sent}, failed={Failed}, total={Total}",
            sent.Count, failed.Count, messages.Count);

        return sent.Count + failed.Count;
    }
}
```

---

## AvtoBus.Outbox.EfCore/OutboxCleanup.cs

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Outbox;

/// <summary>
/// Очистка старых записей outbox и inbox.
/// </summary>
internal sealed class OutboxCleanup : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxCleanup> _log;

    public OutboxCleanup(
        IServiceScopeFactory scopeFactory,
        OutboxOptions options,
        ILogger<OutboxCleanup> log)
    {
        _scopeFactory = scopeFactory;
        _options = options;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromMinutes(15), stoppingToken);

            try
            {
                await using var scope = _scopeFactory.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<DbContext>();
                var cutoff = DateTime.UtcNow - _options.CleanupAfter;

                var deletedOutbox = await db.Set<OutboxMessage>()
                    .Where(o => o.SentAt != null && o.SentAt < cutoff)
                    .ExecuteDeleteAsync(stoppingToken);

                var deletedInbox = await db.Set<InboxRecord>()
                    .Where(i => i.ProcessedAt < cutoff)
                    .ExecuteDeleteAsync(stoppingToken);

                if (deletedOutbox > 0 || deletedInbox > 0)
                    _log.LogInformation("Cleanup: outbox={Outbox}, inbox={Inbox}",
                        deletedOutbox, deletedInbox);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Cleanup error");
            }
        }
    }
}
```

---

## AvtoBus.Outbox.EfCore/OutboxOptions.cs

```csharp
namespace AvtoBus.Outbox;

public sealed class OutboxOptions
{
    /// <summary>
    /// Размер батча для claim.
    /// </summary>
    public int BatchSize { get; set; } = 200;

    /// <summary>
    /// Максимальная параллельность отправки.
    /// </summary>
    public int Parallelism { get; set; } = 8;

    /// <summary>
    /// Интервал polling fallback.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Время жизни отправленных записей перед удалением.
    /// </summary>
    public TimeSpan CleanupAfter { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Время жизни claim-а перед сбросом (для зависших).
    /// </summary>
    public TimeSpan StaleClaimTimeout { get; set; } = TimeSpan.FromMinutes(2);
}
```

---

## AvtoBus.Outbox.EfCore/Registration.cs

```csharp
using AvtoBus.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus;

/// <summary>
/// Extension method для подключения Outbox.
/// </summary>
public static class OutboxRegistration
{
    public static BusOptions UseOutbox<TDbContext>(
        this BusOptions bus,
        Action<OutboxOptions>? configure = null)
        where TDbContext : DbContext
    {
        var options = new OutboxOptions();
        configure?.Invoke(options);

        bus.Services.AddSingleton(options);
        bus.Services.TryAddSingleton<IOutboxSignal, ChannelOutboxSignal>();
        bus.Services.TryAddSingleton<IEnvelopeSerializer, SystemTextJsonEnvelopeSerializer>();
        bus.Services.AddScoped<IOutbox, EfCoreOutbox<TDbContext>>();
        bus.Services.AddHostedService<OutboxRelay>();
        bus.Services.AddHostedService<OutboxCleanup>();

        bus.OutboxOptions = options;
        return bus;
    }
}

public static class InboxRegistration
{
    public static BusOptions UseInboxDeduplication(
        this BusOptions bus,
        TimeSpan? window = null)
    {
        var options = new InboxOptions
        {
            Window = window ?? TimeSpan.FromHours(24)
        };
        bus.InboxOptions = options;

        bus.Services.AddSingleton<IInMemoryCache>(
            new InMemoryDedupCache(options.Window));

        return bus;
    }
}
```

---

## AvtoBus.Outbox.EfCore/EnvelopeSerializer.cs

```csharp
namespace AvtoBus.Outbox;

public interface IEnvelopeSerializer
{
    byte[] Serialize(Envelope envelope);
    Envelope Deserialize(byte[] blob);
}

/// <summary>
/// JSON-сериализация envelope для хранения в БД.
/// </summary>
internal sealed class SystemTextJsonEnvelopeSerializer : IEnvelopeSerializer
{
    private static readonly System.Text.Json.JsonSerializerOptions s_options = new()
    {
        PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    };

    public byte[] Serialize(Envelope envelope)
    {
        var dto = EnvelopeDto.FromEnvelope(envelope);
        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(dto, s_options);
    }

    public Envelope Deserialize(byte[] blob)
    {
        var dto = System.Text.Json.JsonSerializer.Deserialize<EnvelopeDto>(blob, s_options)!;
        return dto.ToEnvelope();
    }
}

/// <summary>
/// DTO для JSON-сериализации envelope.
/// </summary>
internal sealed class EnvelopeDto
{
    public Guid MessageId { get; set; }
    public Guid? CorrelationId { get; set; }
    public Guid? CausationId { get; set; }
    public string MessageType { get; set; } = "";
    public string Body { get; set; } = ""; // base64
    public string ContentType { get; set; } = "application/json";
    public string ContentEncoding { get; set; } = "identity";
    public DateTimeOffset SentAt { get; set; }
    public DateTimeOffset? DeliverAt { get; set; }
    public DateTimeOffset? Deadline { get; set; }
    public double? TimeToLiveMs { get; set; }
    public string? PartitionKey { get; set; }
    public string? TenantId { get; set; }
    public string? ReplyTo { get; set; }
    public string? Source { get; set; }
    public string? Consumer { get; set; }
    public byte Priority { get; set; }
    public int DeliveryAttempt { get; set; }
    public string? TraceParent { get; set; }
    public string? TraceState { get; set; }
    public Dictionary<string, string> Headers { get; set; } = new();

    public static EnvelopeDto FromEnvelope(Envelope e) => new()
    {
        MessageId = e.MessageId,
        CorrelationId = e.CorrelationId,
        CausationId = e.CausationId,
        MessageType = e.MessageType,
        Body = Convert.ToBase64String(e.Body.Span),
        ContentType = e.ContentType,
        ContentEncoding = e.ContentEncoding,
        SentAt = e.SentAt,
        DeliverAt = e.DeliverAt,
        Deadline = e.Deadline,
        TimeToLiveMs = e.TimeToLive?.TotalMilliseconds,
        PartitionKey = e.PartitionKey,
        TenantId = e.TenantId,
        ReplyTo = e.ReplyTo,
        Source = e.Source,
        Consumer = e.Consumer,
        Priority = e.Priority,
        DeliveryAttempt = e.DeliveryAttempt,
        TraceParent = e.TraceParent,
        TraceState = e.TraceState,
        Headers = new Dictionary<string, string>(e.Headers),
    };

    public Envelope ToEnvelope() => new()
    {
        MessageId = MessageId,
        CorrelationId = CorrelationId,
        CausationId = CausationId,
        MessageType = MessageType,
        Body = Convert.FromBase64String(Body),
        ContentType = ContentType,
        ContentEncoding = ContentEncoding,
        SentAt = SentAt,
        DeliverAt = DeliverAt,
        Deadline = Deadline,
        TimeToLive = TimeToLiveMs.HasValue ? TimeSpan.FromMilliseconds(TimeToLiveMs.Value) : null,
        PartitionKey = PartitionKey,
        TenantId = TenantId,
        ReplyTo = ReplyTo,
        Source = Source,
        Consumer = Consumer,
        Priority = Priority,
        DeliveryAttempt = DeliveryAttempt,
        TraceParent = TraceParent,
        TraceState = TraceState,
        Headers = Headers.ToFrozenDictionary(),
    };
}
```
