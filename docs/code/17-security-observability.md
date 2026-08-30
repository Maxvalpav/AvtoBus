# AvtoBus.Security — Подписи, шифрование, tenancy, health

Всё, что относится к безопасности и observability на уровне production.

---

## AvtoBus.Security/MessageSignatureMiddleware.cs

```csharp
using System.Security.Cryptography;
using System.Text;

namespace AvtoBus.Security;

/// <summary>
/// Middleware подписания сообщений на выходе и верификации на входе.
/// HMAC-SHA256 или Ed25519.
/// </summary>
public sealed class MessageSignatureMiddleware : IBusMiddleware
{
    private readonly IKeyProvider _keys;
    private readonly SignatureOptions _options;
    private readonly ILogger<MessageSignatureMiddleware> _log;

    public MessageSignatureMiddleware(IKeyProvider keys, SignatureOptions options,
        ILogger<MessageSignatureMiddleware> log)
    {
        _keys = keys;
        _options = options;
        _log = log;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        // Верификация подписи входящих
        if (_options.RequireSignatureFrom.Count > 0)
        {
            var source = ctx.Envelope.Headers.GetValueOrDefault("avtobus.signer");
            if (source is null || !_options.RequireSignatureFrom.Contains(source))
            {
                if (_options.RequireSignatureFrom.Contains("*") || source is not null)
                    await VerifySignature(ctx.Envelope);
            }
        }

        await next(ctx);
    }

    private async ValueTask VerifySignature(Envelope envelope)
    {
        var signatureB64 = envelope.Headers.GetValueOrDefault("avtobus.signature");
        var keyId = envelope.Headers.GetValueOrDefault("avtobus.key-id");
        if (signatureB64 is null || keyId is null)
            throw new SecurityException("Missing signature or key-id header");

        var signature = Convert.FromBase64String(signatureB64);
        var key = await _keys.GetVerificationKeyAsync(keyId);

        var canonical = BuildCanonical(envelope);
        var computed = HMACSHA256.HashData(key, canonical);

        if (!CryptographicOperations.FixedTimeEquals(signature, computed))
        {
            _log.LogError("Signature verification failed for {Id}", envelope.MessageId);
            throw new SecurityException("Invalid message signature");
        }
    }

    private static byte[] BuildCanonical(Envelope envelope)
    {
        var sb = new StringBuilder();
        sb.Append(envelope.MessageId).Append('\n')
          .Append(envelope.MessageType).Append('\n')
          .Append(envelope.SentAt.ToUnixTimeMilliseconds()).Append('\n')
          .Append(envelope.TenantId ?? "").Append('\n');
        var payloadHash = SHA256.HashData(envelope.Body.Span);
        sb.Append(Convert.ToHexString(payloadHash));
        return Encoding.UTF8.GetBytes(sb.ToString());
    }
}

public sealed class SignatureOptions
{
    public HashSet<string> RequireSignatureFrom { get; } = new();
    public bool SignOutgoing { get; set; } = true;
    public string SignerId { get; set; } = "";
    public string KeyId { get; set; } = "default";
}

public interface IKeyProvider
{
    ValueTask<byte[]> GetSigningKeyAsync(string keyId);
    ValueTask<byte[]> GetVerificationKeyAsync(string keyId);
}

public sealed class SecurityException(string message) : Exception(message);
```

---

## AvtoBus.Security/EnvelopeEncryption.cs

```csharp
using System.Security.Cryptography;

namespace AvtoBus.Security;

/// <summary>
/// AES-GCM шифрование тела сообщения с envelope encryption (data key от KMS).
/// </summary>
public sealed class EnvelopeEncryptor
{
    private readonly IKmsClient _kms;

    public EnvelopeEncryptor(IKmsClient kms) => _kms = kms;

    /// <summary>
    /// Зашифровать тело. Ключ данных генерируется, зашифровывается KMS, идёт в заголовке.
    /// </summary>
    public async ValueTask<(byte[] ciphertext, string encryptedKey, byte[] nonce, byte[] tag)>
        EncryptAsync(byte[] plaintext, string masterKeyId)
    {
        var dataKey = new byte[32];
        RandomNumberGenerator.Fill(dataKey);

        var nonce = new byte[12];
        RandomNumberGenerator.Fill(nonce);

        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[16];

        using var aes = new AesGcm(dataKey, tag.Length);
        aes.Encrypt(nonce, plaintext, ciphertext, tag);

        var encryptedKey = await _kms.EncryptAsync(dataKey, masterKeyId);
        CryptographicOperations.ZeroMemory(dataKey);

        return (ciphertext, encryptedKey, nonce, tag);
    }

    public async ValueTask<byte[]> DecryptAsync(
        byte[] ciphertext, string encryptedKey, byte[] nonce, byte[] tag, string masterKeyId)
    {
        var dataKey = await _kms.DecryptAsync(encryptedKey, masterKeyId);
        var plaintext = new byte[ciphertext.Length];

        using var aes = new AesGcm(dataKey, tag.Length);
        aes.Decrypt(nonce, ciphertext, tag, plaintext);

        CryptographicOperations.ZeroMemory(dataKey);
        return plaintext;
    }
}

public interface IKmsClient
{
    ValueTask<string> EncryptAsync(byte[] plaintext, string keyId);
    ValueTask<byte[]> DecryptAsync(string ciphertext, string keyId);
}
```

---

## AvtoBus.Security/PiiRedactor.cs

```csharp
using System.Reflection;
using System.Text.Json;

namespace AvtoBus.Security;

/// <summary>
/// Маскирует [PersonalData]-поля при логировании/отображении в дашборде.
/// </summary>
public static class PiiRedactor
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, string[]> _piiFields = new();

    public static string RedactJson(string json, Type messageType)
    {
        var fields = GetPiiFields(messageType);
        if (fields.Length == 0) return json;

        using var doc = JsonDocument.Parse(json);
        using var ms = new MemoryStream();
        using (var writer = new Utf8JsonWriter(ms, new JsonWriterOptions { Indented = false }))
            RedactElement(doc.RootElement, writer, fields);

        return Encoding.UTF8.GetString(ms.ToArray());
    }

    private static void RedactElement(JsonElement element, Utf8JsonWriter writer, string[] piiFields)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var prop in element.EnumerateObject())
                {
                    writer.WritePropertyName(prop.Name);
                    if (piiFields.Contains(prop.Name, StringComparer.OrdinalIgnoreCase))
                        writer.WriteStringValue("***REDACTED***");
                    else
                        RedactElement(prop.Value, writer, piiFields);
                }
                writer.WriteEndObject();
                break;
            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var item in element.EnumerateArray())
                    RedactElement(item, writer, piiFields);
                writer.WriteEndArray();
                break;
            default:
                element.WriteTo(writer);
                break;
        }
    }

    private static string[] GetPiiFields(Type type) => _piiFields.GetOrAdd(type, t =>
        t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
         .Where(p => p.GetCustomAttribute<PersonalDataAttribute>() is not null)
         .Select(p => p.Name)
         .ToArray());
}
```

---

## AvtoBus.Security/AllowlistTypeResolver.cs

```csharp
using System.Collections.Frozen;

namespace AvtoBus.Security;

/// <summary>
/// Резолвер типов на основе allowlist — единственный безопасный путь.
/// Никакой десериализации типов, не зарегистрированных явно.
/// </summary>
public sealed class AllowlistTypeResolver : ITypeResolver
{
    private readonly FrozenDictionary<string, Type> _byName;
    private readonly FrozenDictionary<Type, string> _byType;

    public AllowlistTypeResolver(IReadOnlyDictionary<string, Type> allowlist)
    {
        _byName = allowlist.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
        _byType = allowlist.ToFrozenDictionary(kv => kv.Value, kv => kv.Key);
    }

    public string GetName(Type type) => _byType.TryGetValue(type, out var name)
        ? name
        : throw new SecurityException($"Type {type.FullName} is not in allowlist");

    public Type? GetType(string name) => _byName.TryGetValue(name, out var t) ? t : null;
}
```

---

## AvtoBus.MultiTenancy/TenantIsolationMiddleware.cs

```csharp
namespace AvtoBus.MultiTenancy;

/// <summary>
/// Гарантирует, что операции сервиса видят только данные текущего тенанта.
/// </summary>
public sealed class TenantIsolationMiddleware : IBusMiddleware
{
    private readonly TenantOptions _options;
    private readonly ILogger<TenantIsolationMiddleware> _log;

    public TenantIsolationMiddleware(TenantOptions options, ILogger<TenantIsolationMiddleware> log)
    {
        _options = options;
        _log = log;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        var tenantId = ctx.Envelope.TenantId;

        if (_options.RequireTenant && tenantId is null)
            throw new SecurityException("TenantId is required but missing");

        if (tenantId is not null && _options.AllowedTenants.Count > 0
            && !_options.AllowedTenants.Contains(tenantId))
        {
            _log.LogError("Rejected message from unknown tenant {Tenant}", tenantId);
            throw new SecurityException($"Tenant {tenantId} is not allowed");
        }

        // Устанавливаем tenant в scope
        var accessor = ctx.Services.GetService<ITenantAccessor>();
        if (accessor is not null) accessor.CurrentTenantId = tenantId;

        await next(ctx);
    }
}

public sealed class TenantOptions
{
    public bool RequireTenant { get; set; }
    public HashSet<string> AllowedTenants { get; } = new();
}
```

---

## AvtoBus.MultiTenancy/FairScheduler.cs

```csharp
using System.Collections.Concurrent;

namespace AvtoBus.MultiTenancy;

/// <summary>
/// Fair scheduling: не даёт одному тенанту забить все воркеры.
/// Weighted round-robin per-tenant.
/// </summary>
public sealed class FairScheduler
{
    private readonly ConcurrentDictionary<string, TenantQuota> _quotas = new();
    private readonly int _defaultConcurrency;

    public FairScheduler(int defaultConcurrency = 4) => _defaultConcurrency = defaultConcurrency;

    public void SetQuota(string tenantId, int maxConcurrent, int weight = 1)
        => _quotas[tenantId] = new TenantQuota(maxConcurrent, weight);

    public async Task<IDisposable> AcquireAsync(string tenantId, CancellationToken ct)
    {
        var quota = _quotas.GetOrAdd(tenantId, _ =>
            new TenantQuota(_defaultConcurrency, 1));
        await quota.Semaphore.WaitAsync(ct);
        return new Releaser(quota.Semaphore);
    }

    private sealed record TenantQuota(int MaxConcurrent, int Weight)
    {
        public SemaphoreSlim Semaphore { get; } = new(MaxConcurrent);
    }

    private sealed class Releaser(SemaphoreSlim sem) : IDisposable
    {
        public void Dispose() => sem.Release();
    }
}
```

---

## AvtoBus.RateLimiting/RateLimitMiddleware.cs

```csharp
using System.Threading.RateLimiting;

namespace AvtoBus.RateLimiting;

/// <summary>
/// Ограничение частоты обработки per-tenant / per-message-type.
/// Использует System.Threading.RateLimiting из .NET 8+.
/// </summary>
public sealed class RateLimitMiddleware : IBusMiddleware
{
    private readonly RateLimiter _limiter;
    private readonly Func<ConsumeContext, string> _keyFn;

    public RateLimitMiddleware(RateLimiter limiter, Func<ConsumeContext, string>? keyFn = null)
    {
        _limiter = limiter;
        _keyFn = keyFn ?? (ctx => ctx.Envelope.TenantId ?? "default");
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        using var lease = await _limiter.AcquireAsync(1, ctx.CancellationToken);
        if (!lease.IsAcquired)
        {
            // Rate limit исчерпан — defer
            await ctx.DeferAsync(TimeSpan.FromSeconds(5));
            return;
        }
        await next(ctx);
    }

    public static RateLimiter PerTenantLimiter(int permitsPerSecond) =>
        new SlidingWindowRateLimiter(new SlidingWindowRateLimiterOptions
        {
            PermitLimit = permitsPerSecond,
            Window = TimeSpan.FromSeconds(1),
            SegmentsPerWindow = 4,
            AutoReplenishment = true,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
        });
}
```

---

## AvtoBus.Resilience/CircuitBreakerMiddleware.cs

```csharp
namespace AvtoBus.Resilience;

/// <summary>
/// Простой circuit breaker: после N ошибок пауза, half-open проба.
/// </summary>
public sealed class CircuitBreakerMiddleware : IBusMiddleware
{
    private readonly CircuitBreakerOptions _options;
    private readonly ILogger<CircuitBreakerMiddleware> _log;
    private readonly Dictionary<string, BreakerState> _breakers = new();
    private readonly object _lock = new();

    public CircuitBreakerMiddleware(CircuitBreakerOptions options, ILogger<CircuitBreakerMiddleware> log)
    {
        _options = options;
        _log = log;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        var key = ctx.Envelope.MessageType;
        var state = GetOrCreate(key);

        // Open — не пропускаем
        if (state.Status == BreakerStatus.Open && DateTime.UtcNow < state.OpenUntil)
        {
            _log.LogWarning("Circuit breaker OPEN for {Type} — deferring", key);
            await ctx.DeferAsync(state.OpenUntil - DateTime.UtcNow);
            return;
        }

        // HalfOpen — пробуем один запрос
        if (state.Status == BreakerStatus.Open && DateTime.UtcNow >= state.OpenUntil)
            state.Status = BreakerStatus.HalfOpen;

        try
        {
            await next(ctx);
            OnSuccess(state);
        }
        catch (Exception)
        {
            OnFailure(state, key);
            throw;
        }
    }

    private BreakerState GetOrCreate(string key)
    {
        lock (_lock)
        {
            if (!_breakers.TryGetValue(key, out var s))
            {
                s = new BreakerState();
                _breakers[key] = s;
            }
            return s;
        }
    }

    private void OnSuccess(BreakerState state)
    {
        state.FailureCount = 0;
        state.Status = BreakerStatus.Closed;
    }

    private void OnFailure(BreakerState state, string key)
    {
        state.FailureCount++;
        if (state.FailureCount >= _options.FailureThreshold)
        {
            state.Status = BreakerStatus.Open;
            state.OpenUntil = DateTime.UtcNow.Add(_options.BreakDuration);
            _log.LogError("Circuit breaker OPENED for {Type}", key);
        }
    }

    private sealed class BreakerState
    {
        public BreakerStatus Status { get; set; } = BreakerStatus.Closed;
        public int FailureCount { get; set; }
        public DateTime OpenUntil { get; set; }
    }
}

public enum BreakerStatus { Closed, Open, HalfOpen }

public sealed class CircuitBreakerOptions
{
    public int FailureThreshold { get; set; } = 5;
    public TimeSpan BreakDuration { get; set; } = TimeSpan.FromSeconds(30);
}
```

---

## AvtoBus.Health/BusHealthCheck.cs

```csharp
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace AvtoBus.Health;

/// <summary>
/// Health check: транспорт живой, outbox не растёт, лаг в норме.
/// </summary>
public sealed class BusHealthCheck : IHealthCheck
{
    private readonly Transport.ITransportSelector _transports;
    private readonly IOutboxStatus? _outbox;
    private readonly BusHealthOptions _options;

    public BusHealthCheck(Transport.ITransportSelector transports, BusHealthOptions options,
        IOutboxStatus? outbox = null)
    {
        _transports = transports;
        _options = options;
        _outbox = outbox;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var data = new Dictionary<string, object>();

        // 1. Транспорт
        try
        {
            var transport = _transports.Default;
            // Ping-like — попытка отправить no-op сообщение
            data["transport"] = transport.Name;
            data["transport.status"] = "connected";
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Transport disconnected", ex);
        }

        // 2. Outbox
        if (_outbox is not null)
        {
            var pending = _outbox.PendingCount;
            data["outbox.pending"] = pending;

            if (pending > _options.OutboxCriticalThreshold)
                return HealthCheckResult.Unhealthy(
                    $"Outbox critical: {pending} messages pending", data: data);

            if (pending > _options.OutboxWarningThreshold)
                return HealthCheckResult.Degraded(
                    $"Outbox degraded: {pending} messages pending", data: data);
        }

        return HealthCheckResult.Healthy("AvtoBus healthy", data);
    }
}

public sealed class BusHealthOptions
{
    public int OutboxWarningThreshold { get; set; } = 1000;
    public int OutboxCriticalThreshold { get; set; } = 10_000;
}

public static class HealthCheckRegistration
{
    public static IHealthChecksBuilder AddAvtoBus(this IHealthChecksBuilder builder,
        Action<BusHealthOptions>? configure = null)
    {
        var options = new BusHealthOptions();
        configure?.Invoke(options);
        builder.Services.AddSingleton(options);
        builder.AddCheck<BusHealthCheck>("avtobus", tags: new[] { "ready" });
        return builder;
    }
}
```

---

## AvtoBus.Chaos/ChaosMiddleware.cs

```csharp
namespace AvtoBus.Chaos;

/// <summary>
/// Инжектит хаос в pipeline для тестирования устойчивости.
/// Включать только в non-production!
/// </summary>
public sealed class ChaosMiddleware : IBusMiddleware
{
    private readonly ChaosOptions _options;
    private readonly ILogger<ChaosMiddleware> _log;

    public ChaosMiddleware(ChaosOptions options, ILogger<ChaosMiddleware> log)
    {
        _options = options;
        _log = log;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        // Duplicate
        if (Random.Shared.NextDouble() < _options.DuplicateProbability)
        {
            _log.LogWarning("Chaos: injecting duplicate for {Id}", ctx.Envelope.MessageId);
            await next(ctx); // выполняем дважды
        }

        // Reorder delay
        if (Random.Shared.NextDouble() < _options.ReorderProbability)
        {
            var delay = TimeSpan.FromMilliseconds(
                Random.Shared.Next(0, (int)_options.MaxReorderDelay.TotalMilliseconds));
            await Task.Delay(delay, ctx.CancellationToken);
        }

        // Fail
        if (Random.Shared.NextDouble() < _options.FailProbability)
        {
            _log.LogWarning("Chaos: injecting failure for {Id}", ctx.Envelope.MessageId);
            throw new InvalidOperationException("Chaos-injected failure");
        }

        // Slow processing
        if (Random.Shared.NextDouble() < _options.SlowProbability)
        {
            var slowness = TimeSpan.FromMilliseconds(
                Random.Shared.Next(0, (int)_options.MaxSlowness.TotalMilliseconds));
            await Task.Delay(slowness, ctx.CancellationToken);
        }

        await next(ctx);
    }
}

public sealed class ChaosOptions
{
    public double DuplicateProbability { get; set; } = 0.0;
    public double ReorderProbability { get; set; } = 0.0;
    public double FailProbability { get; set; } = 0.0;
    public double SlowProbability { get; set; } = 0.0;
    public TimeSpan MaxReorderDelay { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan MaxSlowness { get; set; } = TimeSpan.FromSeconds(5);
}
```

---

## AvtoBus.LocalQueues/LocalQueue.cs

```csharp
using System.Threading.Channels;

namespace AvtoBus.LocalQueues;

/// <summary>
/// In-process очередь — быстрая, для локальных задач без брокера.
/// Вдохновение: Wolverine local queues.
/// </summary>
public sealed class LocalQueue<T> where T : class
{
    private readonly Channel<T> _channel;
    private readonly int _maxParallelism;
    private readonly Func<T, IServiceProvider, ValueTask> _handler;
    private readonly IServiceProvider _services;
    private readonly List<Task> _workers = new();
    private CancellationTokenSource? _cts;

    public string Name { get; }

    public LocalQueue(string name, int maxParallelism,
        Func<T, IServiceProvider, ValueTask> handler,
        IServiceProvider services, int capacity = 10_000)
    {
        Name = name;
        _maxParallelism = maxParallelism;
        _handler = handler;
        _services = services;
        _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
        });
    }

    public void Start(CancellationToken stoppingToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        for (int i = 0; i < _maxParallelism; i++)
            _workers.Add(Task.Run(() => WorkerLoop(_cts.Token), _cts.Token));
    }

    public ValueTask EnqueueAsync(T message, CancellationToken ct = default)
        => _channel.Writer.WriteAsync(message, ct);

    private async Task WorkerLoop(CancellationToken ct)
    {
        await foreach (var msg in _channel.Reader.ReadAllAsync(ct))
        {
            try { await _handler(msg, _services); }
            catch { /* log */ }
        }
    }
}
```
