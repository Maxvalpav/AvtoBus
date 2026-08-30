# AvtoBus — Hosting, конфигурация, наблюдаемость

> **Code sketch / unverified.** Конфигурация, hosting и OTel должны проверяться в реальном Generic Host. Канонический статус: [`../FINAL.md`](../FINAL.md).

Исправление B5–B7, B9, B12 и A4 из `30-forgotten-and-bugs.md`.

---

## 1. Конфигурация через appsettings.json + IOptions (B5)

### appsettings.json

```json
{
  "AvtoBus": {
    "EndpointName": "orders-service",
    "DefaultTransport": "rabbitmq",
    "Transports": {
      "rabbitmq": {
        "ConnectionString": "amqp://guest:guest@localhost:5672",
        "PrefetchCount": 64,
        "UseQuorumQueues": true,
        "DeliveryLimit": 6
      },
      "kafka": {
        "BootstrapServers": "localhost:9092",
        "ExactlyOnce": false
      }
    },
    "Recoverability": {
      "ImmediateRetries": 3,
      "DelayedRetries": 5,
      "DelayedBackoffBaseSeconds": 5,
      "DelayedBackoffMaxSeconds": 300
    },
    "Outbox": {
      "BatchSize": 200,
      "Parallelism": 8,
      "PollIntervalSeconds": 3,
      "CleanupAfterDays": 7
    },
    "Inbox": {
      "WindowHours": 24
    },
    "Health": {
      "MaxOutboxPending": 10000,
      "MaxOutboxAgeSeconds": 300
    }
  }
}
```

### AvtoBus.Core/Configuration/AvtoBusConfiguration.cs

```csharp
namespace AvtoBus.Configuration;

/// <summary>
/// Корневая секция конфигурации AvtoBus.
/// </summary>
public sealed class AvtoBusConfiguration
{
    public const string SectionName = "AvtoBus";

    public string EndpointName { get; set; } = "avtobus";
    public string DefaultTransport { get; set; } = "inmemory";
    public int DefaultPrefetch { get; set; } = 32;
    public int DefaultMaxParallelism { get; set; } = Environment.ProcessorCount;

    public Dictionary<string, TransportConfig> Transports { get; set; } = new();
    public RecoverabilityConfig Recoverability { get; set; } = new();
    public OutboxConfig Outbox { get; set; } = new();
    public InboxConfig Inbox { get; set; } = new();
    public HealthConfig Health { get; set; } = new();
}

public sealed class TransportConfig
{
    public string? ConnectionString { get; set; }
    public string? BootstrapServers { get; set; }
    public int PrefetchCount { get; set; } = 64;
    public bool UseQuorumQueues { get; set; } = true;
    public int DeliveryLimit { get; set; } = 6;
    public bool ExactlyOnce { get; set; }
}

public sealed class RecoverabilityConfig
{
    public int ImmediateRetries { get; set; } = 3;
    public int DelayedRetries { get; set; } = 5;
    public double DelayedBackoffBaseSeconds { get; set; } = 5;
    public double DelayedBackoffMaxSeconds { get; set; } = 300;
}

public sealed class OutboxConfig
{
    public int BatchSize { get; set; } = 200;
    public int Parallelism { get; set; } = 8;
    public int PollIntervalSeconds { get; set; } = 3;
    public int CleanupAfterDays { get; set; } = 7;
}

public sealed class InboxConfig
{
    public int WindowHours { get; set; } = 24;
}

public sealed class HealthConfig
{
    public int MaxOutboxPending { get; set; } = 10_000;
    public int MaxOutboxAgeSeconds { get; set; } = 300;
}
```

### Валидация опций (fail-fast при старте)

```csharp
using Microsoft.Extensions.Options;

namespace AvtoBus.Configuration;

[OptionsValidator]
public sealed partial class ValidateAvtoBusConfiguration
    : IValidateOptions<AvtoBusConfiguration>;

/// <summary>
/// Ручная валидация с понятными сообщениями (собирает ВСЕ ошибки сразу, идея 421).
/// </summary>
internal sealed class AvtoBusConfigValidator : IValidateOptions<AvtoBusConfiguration>
{
    public ValidateOptionsResult Validate(string? name, AvtoBusConfiguration options)
    {
        var errors = new List<string>();

        if (string.IsNullOrWhiteSpace(options.EndpointName))
            errors.Add("AvtoBus:EndpointName is required.");

        if (options.DefaultTransport != "inmemory"
            && !options.Transports.ContainsKey(options.DefaultTransport))
            errors.Add($"AvtoBus:DefaultTransport '{options.DefaultTransport}' has no matching Transports entry.");

        foreach (var (name2, transport) in options.Transports)
        {
            if (name2 is "rabbitmq" && string.IsNullOrEmpty(transport.ConnectionString))
                errors.Add($"AvtoBus:Transports:{name2}:ConnectionString is required.");
            if (name2 is "kafka" && string.IsNullOrEmpty(transport.BootstrapServers))
                errors.Add($"AvtoBus:Transports:{name2}:BootstrapServers is required.");
        }

        if (options.Recoverability.ImmediateRetries < 0)
            errors.Add("AvtoBus:Recoverability:ImmediateRetries must be >= 0.");

        return errors.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(errors);
    }
}
```

### Биндинг из конфигурации

```csharp
public static IServiceCollection AddAvtoBus(
    this IServiceCollection services,
    IConfiguration configuration,
    Action<BusOptions>? configure = null)
{
    services
        .AddOptions<AvtoBusConfiguration>()
        .Bind(configuration.GetSection(AvtoBusConfiguration.SectionName))
        .ValidateOnStart();

    services.AddSingleton<IValidateOptions<AvtoBusConfiguration>, AvtoBusConfigValidator>();

    var config = configuration.GetSection(AvtoBusConfiguration.SectionName)
        .Get<AvtoBusConfiguration>() ?? new();

    return services.AddAvtoBus(bus =>
    {
        bus.EndpointName = config.EndpointName;
        bus.DefaultPrefetch = config.DefaultPrefetch;
        bus.Recoverability(r => r
            .ImmediateRetries(config.Recoverability.ImmediateRetries)
            .DelayedRetries(config.Recoverability.DelayedRetries,
                TimeSpan.FromSeconds(config.Recoverability.DelayedBackoffBaseSeconds)));
        configure?.Invoke(bus);
    });
}
```

---

## 2. OpenTelemetry setup-extension (B6)

### AvtoBus.Core/Observability/OpenTelemetryExtensions.cs

```csharp
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace AvtoBus;

/// <summary>
/// Регистрация AvtoBus в OpenTelemetry.
/// Без этого трейсы и метрики шины не будут экспортироваться.
/// </summary>
public static class AvtoBusOpenTelemetryExtensions
{
    /// <summary>Добавить трейсинг AvtoBus.</summary>
    public static TracerProviderBuilder AddAvtoBusInstrumentation(this TracerProviderBuilder builder)
        => builder.AddSource(BusTracing.Source.Name);

    /// <summary>Добавить метрики AvtoBus.</summary>
    public static MeterProviderBuilder AddAvtoBusInstrumentation(this MeterProviderBuilder builder)
        => builder.AddMeter(BusMetrics.MeterName);
}
```

Использование:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t
        .AddAvtoBusInstrumentation()        // ← трейсы шины
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter())
    .WithMetrics(m => m
        .AddAvtoBusInstrumentation()        // ← метрики шины
        .AddPrometheusExporter());
```

---

## 3. Graceful shutdown / draining (B7)

### AvtoBus.Core/Hosting/GracefulBusHost.cs

```csharp
using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;

namespace AvtoBus.Hosting;

/// <summary>
/// Управление graceful shutdown: остановить приём, дождаться in-flight, вернуть недоделанное.
/// </summary>
public sealed class DrainCoordinator
{
    private readonly ConcurrentDictionary<Guid, byte> _inFlight = new();
    private volatile bool _acceptingNew = true;
    private readonly TimeSpan _drainTimeout;
    private readonly ILogger<DrainCoordinator> _log;

    public DrainCoordinator(TimeSpan drainTimeout, ILogger<DrainCoordinator> log)
    {
        _drainTimeout = drainTimeout;
        _log = log;
    }

    public bool AcceptingNew => _acceptingNew;
    public int InFlightCount => _inFlight.Count;

    public IDisposable? TrackInFlight(Guid messageId)
    {
        if (!_acceptingNew) return null;
        _inFlight[messageId] = 0;
        return new InFlightScope(this, messageId);
    }

    public async Task DrainAsync(CancellationToken ct)
    {
        _acceptingNew = false;   // 1. Стоп приём новых
        _log.LogInformation("Draining: {Count} in-flight messages", _inFlight.Count);

        var deadline = DateTime.UtcNow + _drainTimeout;
        while (_inFlight.Count > 0 && DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
        {
            await Task.Delay(100, ct);
        }

        if (_inFlight.Count > 0)
            _log.LogWarning("Drain timeout: {Count} messages will be redelivered", _inFlight.Count);
        else
            _log.LogInformation("Drain complete: all messages processed");
    }

    private void Complete(Guid id) => _inFlight.TryRemove(id, out _);

    private sealed class InFlightScope(DrainCoordinator coordinator, Guid id) : IDisposable
    {
        public void Dispose() => coordinator.Complete(id);
    }
}

/// <summary>
/// Интеграция с жизненным циклом хоста: вызывает drain при остановке.
/// </summary>
internal sealed class DrainHostedService : IHostedService
{
    private readonly DrainCoordinator _coordinator;
    private readonly IHostApplicationLifetime _lifetime;

    public DrainHostedService(DrainCoordinator coordinator, IHostApplicationLifetime lifetime)
    {
        _coordinator = coordinator;
        _lifetime = lifetime;
    }

    public Task StartAsync(CancellationToken ct) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken ct)
    {
        // ct обычно = terminationGracePeriod из Kubernetes
        await _coordinator.DrainAsync(ct);
    }
}
```

Интеграция в `BusHost.ProcessMessage`:

```csharp
using var drainScope = _drainCoordinator.TrackInFlight(envelope.MessageId);
if (drainScope is null)
{
    // Уже дренируемся — вернуть в очередь, обработает другая реплика
    await transportMsg.Ack.NackAsync(requeue: true, ct);
    return;
}
// ... обработка ...
```

---

## 4. HandlerTimeout enforcement (B9)

### AvtoBus.Core/Pipeline/TimeoutMiddleware.cs

```csharp
using System.Collections.Concurrent;
using System.Reflection;
using AvtoBus.Pipeline;

namespace AvtoBus.Pipeline;

/// <summary>
/// Применяет [HandlerTimeout] — прерывает зависшие хендлеры.
/// </summary>
internal sealed class TimeoutMiddleware : IBusMiddleware
{
    private readonly DispatcherRegistry _dispatchers;
    private static readonly ConcurrentDictionary<Type, TimeSpan?> s_cache = new();

    public TimeoutMiddleware(DispatcherRegistry dispatchers) => _dispatchers = dispatchers;

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        var timeout = GetTimeout(ctx.Message.GetType());
        if (timeout is null)
        {
            await next(ctx);
            return;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ctx.CancellationToken);
        cts.CancelAfter(timeout.Value);

        var timedContext = new ConsumeContext
        {
            Envelope = ctx.Envelope,
            Message = ctx.Message,
            Services = ctx.Services,
            CancellationToken = cts.Token,   // ← взведённый токен
            StartedAt = ctx.StartedAt,
        };

        try
        {
            await next(timedContext);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested && !ctx.CancellationToken.IsCancellationRequested)
        {
            throw new HandlerTimeoutException(ctx.Envelope.MessageType, timeout.Value);
        }
    }

    private TimeSpan? GetTimeout(Type messageType) => s_cache.GetOrAdd(messageType, t =>
    {
        if (!_dispatchers.TryGet(t, out var dispatcher))
            return null;

        // Атрибут может быть на методе или классе-хендлере
        var attr = dispatcher.ClrType.GetCustomAttribute<HandlerTimeoutAttribute>();
        return attr?.Timeout;
    });
}

public sealed class HandlerTimeoutException(string messageType, TimeSpan timeout)
    : Exception($"Handler for '{messageType}' exceeded timeout {timeout}");
```

---

## 5. Migrations-hosted-service (B12)

### AvtoBus.Core/Migrations/SchemaMigrator.cs

```csharp
using Microsoft.Extensions.Hosting;

namespace AvtoBus.Migrations;

/// <summary>
/// Применяет SQL-схемы модулей (outbox, ES, scheduling) при старте.
/// Идемпотентно (CREATE TABLE IF NOT EXISTS) + версионирование через schema-таблицу.
/// </summary>
public interface ISchemaMigration
{
    string ModuleName { get; }
    int Version { get; }
    string Sql { get; }
}

public sealed class SchemaMigrator : IHostedService
{
    private readonly IEnumerable<ISchemaMigration> _migrations;
    private readonly ISchemaExecutor _executor;
    private readonly ILogger<SchemaMigrator> _log;

    public SchemaMigrator(
        IEnumerable<ISchemaMigration> migrations,
        ISchemaExecutor executor,
        ILogger<SchemaMigrator> log)
    {
        _migrations = migrations;
        _executor = executor;
        _log = log;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        await _executor.EnsureSchemaTableAsync(ct);

        foreach (var migration in _migrations.OrderBy(m => m.ModuleName).ThenBy(m => m.Version))
        {
            var applied = await _executor.GetVersionAsync(migration.ModuleName, ct);
            if (applied >= migration.Version)
            {
                _log.LogDebug("Schema {Module} already at v{Version}", migration.ModuleName, applied);
                continue;
            }

            _log.LogInformation("Applying schema {Module} v{Version}", migration.ModuleName, migration.Version);
            await _executor.ExecuteAsync(migration.Sql, ct);
            await _executor.SetVersionAsync(migration.ModuleName, migration.Version, ct);
        }
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}

public interface ISchemaExecutor
{
    ValueTask EnsureSchemaTableAsync(CancellationToken ct);
    ValueTask<int> GetVersionAsync(string module, CancellationToken ct);
    ValueTask SetVersionAsync(string module, int version, CancellationToken ct);
    ValueTask ExecuteAsync(string sql, CancellationToken ct);
}

/// <summary>PostgreSQL-исполнитель схем.</summary>
public sealed class PostgresSchemaExecutor : ISchemaExecutor
{
    private readonly Npgsql.NpgsqlDataSource _dataSource;
    public PostgresSchemaExecutor(Npgsql.NpgsqlDataSource dataSource) => _dataSource = dataSource;

    public async ValueTask EnsureSchemaTableAsync(CancellationToken ct)
    {
        await ExecuteAsync("""
            CREATE TABLE IF NOT EXISTS avtobus_schema_versions (
                module  TEXT PRIMARY KEY,
                version INT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """, ct);
    }

    public async ValueTask<int> GetVersionAsync(string module, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new Npgsql.NpgsqlCommand(
            "SELECT version FROM avtobus_schema_versions WHERE module = @m", conn);
        cmd.Parameters.AddWithValue("m", module);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is int v ? v : 0;
    }

    public async ValueTask SetVersionAsync(string module, int version, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new Npgsql.NpgsqlCommand("""
            INSERT INTO avtobus_schema_versions (module, version, applied_at)
            VALUES (@m, @v, now())
            ON CONFLICT (module) DO UPDATE SET version = @v, applied_at = now()
            """, conn);
        cmd.Parameters.AddWithValue("m", module);
        cmd.Parameters.AddWithValue("v", version);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async ValueTask ExecuteAsync(string sql, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new Npgsql.NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```

---

## 6. LoggerMessage source-gen (фикс A4)

Вместо интерполяции — высокопроизводительные делегаты (не аллоцируют при выключенном уровне):

### AvtoBus.Core/Logging/Log.cs

```csharp
using Microsoft.Extensions.Logging;

namespace AvtoBus;

/// <summary>
/// Все логи шины через source-generated делегаты (CA1848-compliant).
/// </summary>
internal static partial class Log
{
    [LoggerMessage(Level = LogLevel.Debug,
        Message = "Publishing {MessageType} ({MessageId})")]
    public static partial void Publishing(this ILogger logger, string messageType, Guid messageId);

    [LoggerMessage(Level = LogLevel.Debug,
        Message = "Dispatching {MessageType} (attempt {Attempt})")]
    public static partial void Dispatching(this ILogger logger, string messageType, int attempt);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Retry {Attempt}/{Max} for {MessageType}")]
    public static partial void Retrying(this ILogger logger, int attempt, int max, string messageType);

    [LoggerMessage(Level = LogLevel.Error,
        Message = "Permanent error for {MessageType} after {Attempts} attempts")]
    public static partial void PermanentError(this ILogger logger, Exception ex, string messageType, int attempts);

    [LoggerMessage(Level = LogLevel.Warning,
        Message = "Message dead-lettered: {MessageType} — {Reason}")]
    public static partial void DeadLettered(this ILogger logger, string messageType, string reason);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Outbox relay: sent={Sent}, failed={Failed}")]
    public static partial void OutboxRelay(this ILogger logger, int sent, int failed);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "Saga {Saga} {InstanceId} completed")]
    public static partial void SagaCompleted(this ILogger logger, string saga, Guid instanceId);
}
```

Использование: `_log.Publishing(envelope.MessageType, envelope.MessageId);` — 0 аллокаций.

---

## 7. Health-checks registration

```csharp
public static IHealthChecksBuilder AddAvtoBus(
    this IHealthChecksBuilder builder,
    string name = "avtobus")
{
    return builder.AddCheck<AvtoBus.HealthChecks.BusHealthCheck>(
        name,
        tags: new[] { "ready", "messaging" });
}
```

Использование:

```csharp
builder.Services.AddHealthChecks()
    .AddAvtoBus();

app.MapHealthChecks("/healthz");   // liveness
app.MapHealthChecks("/readyz", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready")   // readiness — брокер+outbox
});
```

---

## 8. Полная сборка приложения (то, как это выглядит вместе)

```csharp
var builder = WebApplication.CreateBuilder(args);

// 1. AvtoBus из конфигурации
builder.Services.AddAvtoBus(builder.Configuration, bus =>
{
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
});

// 2. Транспорт (в пакете AvtoBus.RabbitMq, фикс A5)
builder.Services.AddAvtoBusRabbitMq(builder.Configuration);

// 3. БД + транзакционный outbox
builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(cs));
builder.Services.AddAvtoBus(bus =>
{
    bus.UseOutbox<AppDbContext>();
    bus.UseInboxDeduplication();
    bus.UseTransactionalMessaging<AppDbContext>();
});

// 4. Наблюдаемость
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddAvtoBusInstrumentation().AddOtlpExporter())
    .WithMetrics(m => m.AddAvtoBusInstrumentation().AddPrometheusExporter());

// 5. Health
builder.Services.AddHealthChecks().AddAvtoBus();

var app = builder.Build();

app.MapAvtoBusDashboard("/bus");
app.MapHealthChecks("/healthz");
app.MapHealthChecks("/readyz", new() { Predicate = c => c.Tags.Contains("ready") });
app.MapPrometheusScrapingEndpoint();

app.Run();
```
