# AvtoBus.Core — Стандартные middleware

> **Code sketch / unverified.** В проекте должен остаться один согласованный pipeline. Канонический статус: [`../FINAL.md`](../FINAL.md).

---

## AvtoBus.Core/Pipeline/TelemetryMiddleware.cs

```csharp
using System.Diagnostics;

namespace AvtoBus.Pipeline;

/// <summary>
/// Создаёт OTel Activity для каждого сообщения.
/// </summary>
internal sealed class TelemetryMiddleware : IBusMiddleware
{
    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        using var activity = BusTracing.StartConsume(ctx.Envelope);
        try
        {
            await next(ctx);
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity?.AddException(ex);
            throw;
        }
    }
}
```

---

## AvtoBus.Core/Pipeline/ScopeMiddleware.cs

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Pipeline;

/// <summary>
/// Создаёт изолированный DI scope для каждого сообщения.
/// Каждое сообщение получает свой scope — scoped-сервисы (DbContext и т.д.) безопасны.
/// </summary>
internal sealed class ScopeMiddleware : IBusMiddleware
{
    private readonly IServiceScopeFactory _scopeFactory;

    public ScopeMiddleware(IServiceScopeFactory scopeFactory)
    {
        _scopeFactory = scopeFactory;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var sp = scope.ServiceProvider;

        // Создаём новый ConsumeContext с scoped-провайдером
        var scoped = new ConsumeContext
        {
            Envelope = ctx.Envelope,
            Message = ctx.Message,
            Services = sp,
            CancellationToken = ctx.CancellationToken,
            StartedAt = ctx.StartedAt,
        };

        // Устанавливаем как текущий
        var accessor = sp.GetService<IBusContextAccessor>();
        if (accessor is not null)
            accessor.Current = scoped;

        // Копируем Items
        foreach (var item in ctx.Items)
            scoped.Items[item.Key] = item.Value;

        await next(scoped);
    }
}
```

---

## AvtoBus.Core/Pipeline/TenantMiddleware.cs

```csharp
namespace AvtoBus.Pipeline;

/// <summary>
/// Резолвит TenantId из конверта и устанавливает в контекст.
/// </summary>
internal sealed class TenantMiddleware : IBusMiddleware
{
    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        var tenantId = ctx.Envelope.TenantId;
        if (tenantId is not null)
        {
            // Устанавливаем в scope
            var tenantAccessor = ctx.Services.GetService<ITenantAccessor>();
            if (tenantAccessor is not null)
                tenantAccessor.CurrentTenantId = tenantId;

            // Устанавливаем в контекст для хендлеров
            ctx.Items["TenantId"] = tenantId;
        }

        await next(ctx);
    }
}

/// <summary>
/// Доступ к текущему TenantId для хендлеров и сервисов.
/// </summary>
public interface ITenantAccessor
{
    string? CurrentTenantId { get; set; }
}

internal sealed class AsyncLocalTenantAccessor : ITenantAccessor
{
    private static readonly AsyncLocal<string?> _tenantId = new();
    public string? CurrentTenantId
    {
        get => _tenantId.Value;
        set => _tenantId.Value = value;
    }
}
```

---

## AvtoBus.Core/Pipeline/InboxDedupMiddleware.cs

```csharp
using System.Collections.Concurrent;

namespace AvtoBus.Pipeline;

/// <summary>
/// Дедупликация входящих сообщений.
/// Проверяет MessageId по in-memory кэшу + (опционально) БД.
/// </summary>
internal sealed class InboxDedupMiddleware : IBusMiddleware
{
    private readonly InboxOptions? _options;
    private readonly IInMemoryCache? _cache;
    private readonly IInboxStore? _store;

    public InboxDedupMiddleware(
        InboxOptions? options,
        IInMemoryCache? cache = null,
        IInboxStore? store = null)
    {
        _options = options;
        _cache = cache;
        _store = store;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        if (_options is null)
        {
            await next(ctx);
            return;
        }

        var messageId = ctx.Envelope.MessageId;
        var consumerId = ctx.Envelope.Consumer ?? ctx.Envelope.Headers.GetValueOrDefault("consumer") ?? "default";

        // 1. Быстрая проверка по in-memory кэшу
        if (_cache is not null)
        {
            if (_cache.TryMarkProcessing(messageId, consumerId))
            {
                // Уже обрабатывается или обработано
                BusMetrics.InboxDeduped.Add(1);
                return; // duplicate
            }
        }

        // 2. Проверка по БД (если есть)
        if (_store is not null)
        {
            if (!await _store.TryClaimAsync(messageId, consumerId, _options.Window))
            {
                BusMetrics.InboxDeduped.Add(1);
                return; // duplicate
            }
        }

        // 3. Продолжаем обработку
        await next(ctx);

        // 4. После успешной обработки — помечаем как завершённое
        if (_cache is not null)
            _cache.MarkCompleted(messageId, consumerId);
        if (_store is not null)
            await _store.MarkCompletedAsync(messageId, consumerId);
    }
}

/// <summary>
/// Настройки inbox-дедупликации.
/// </summary>
public sealed class InboxOptions
{
    /// <summary>
    /// Окно дедупликации. Сообщения старше этого окна очищаются.
    /// </summary>
    public TimeSpan Window { get; set; } = TimeSpan.FromHours(24);
}

public interface IInMemoryCache
{
    bool TryMarkProcessing(Guid messageId, string consumerId);
    void MarkCompleted(Guid messageId, string consumerId);
}

/// <summary>
/// Простой in-memory кэш дедупликации с TTL.
/// </summary>
internal sealed class InMemoryDedupCache : IInMemoryCache
{
    private sealed class Entry
    {
        public bool IsCompleted { get; set; }
        public DateTime ExpiresAt { get; set; }
    }

    private readonly ConcurrentDictionary<string, Entry> _entries = new();
    private readonly TimeSpan _window;

    public InMemoryDedupCache(TimeSpan window) => _window = window;

    public bool TryMarkProcessing(Guid messageId, string consumerId)
    {
        var key = $"{messageId}:{consumerId}";
        var now = DateTime.UtcNow;

        return _entries.AddOrUpdate(key,
            _ => new Entry { ExpiresAt = now.Add(_window) },
            (_, existing) =>
            {
                if (existing.IsCompleted || existing.ExpiresAt > now)
                    return existing; // duplicate
                return new Entry { ExpiresAt = now.Add(_window) }; // expired, allow
            }).IsCompleted;
    }

    public void MarkCompleted(Guid messageId, string consumerId)
    {
        var key = $"{messageId}:{consumerId}";
        if (_entries.TryGetValue(key, out var entry))
            entry.IsCompleted = true;
    }
}

public interface IInboxStore
{
    ValueTask<bool> TryClaimAsync(Guid messageId, string consumerId, TimeSpan window);
    ValueTask MarkCompletedAsync(Guid messageId, string consumerId);
}
```

---

## AvtoBus.Core/Pipeline/RecoverabilityMiddleware.cs

```csharp
namespace AvtoBus.Pipeline;

/// <summary>
/// Классифицирует исключения и определяет судьбу сообщения:
/// retry immediate, retry delayed, discard, или rethrow (→ DLQ).
/// </summary>
internal sealed class RecoverabilityMiddleware : IBusMiddleware
{
    private readonly RecoverabilityOptions _options;
    private readonly ILogger<RecoverabilityMiddleware> _log;

    public RecoverabilityMiddleware(
        RecoverabilityOptions options,
        ILogger<RecoverabilityMiddleware> log)
    {
        _options = options;
        _log = log;
    }

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        try
        {
            await next(ctx);
        }
        catch (DeadLetterException)
        {
            throw; // Already explicitly dead-lettered
        }
        catch (OperationCanceledException) when (ctx.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            var action = Classify(ex, ctx.Attempt);
            ctx.Items["AvtoBus.RetryAction"] = action;
            throw;
        }
    }

    private FailureAction Classify(Exception ex, int attempt)
    {
        // 1. Явно помеченные исключения
        foreach (var rule in _options.ExceptionRules)
        {
            if (rule.ExceptionType.IsAssignableFrom(ex.GetType()))
            {
                _log.LogDebug("Exception {ExType} matched rule: {Action}", ex.GetType().Name, rule.Action);
                return rule.Action;
            }
        }

        // 2. Immediate retries
        if (attempt < _options.ImmediateRetries)
            return FailureAction.RetryImmediate;

        // 3. Delayed retries
        var delayedAttempt = attempt - _options.ImmediateRetries;
        if (delayedAttempt < _options.DelayedRetries)
            return FailureAction.RetryDelayed;

        // 4. По умолчанию — DLQ
        return FailureAction.DeadLetter;
    }
}

/// <summary>
/// Опции восстановления.
/// </summary>
public sealed class RecoverabilityOptions
{
    public int ImmediateRetries { get; set; } = 3;
    public int DelayedRetries { get; set; } = 5;
    public double DelayedBackoffBaseSeconds { get; set; } = 5;
    public double DelayedBackoffMaxSeconds { get; set; } = 300;

    public List<ExceptionRule> ExceptionRules { get; } = new();

    /// <summary>
    /// Добавить правило для конкретного исключения.
    /// </summary>
    public void MapException<TException>(FailureAction action) where TException : Exception
    {
        ExceptionRules.Add(new ExceptionRule(typeof(TException), action));
    }
}

public sealed record ExceptionRule(Type ExceptionType, FailureAction Action);

public enum FailureAction
{
    /// <summary>Мгновенный повтор (in-memory).</summary>
    RetryImmediate,
    /// <summary>Повтор через задержку (retry-очередь).</summary>
    RetryDelayed,
    /// <summary>Отправить в DLQ.</summary>
    DeadLetter,
    /// <summary>Тихо дропнуть.</summary>
    Discard,
    /// <summary>Подавить исключение, вернуть в основную очередь.</summary>
    Requeue,
}

/// <summary>
/// Fluent builder для RecoverabilityOptions.
/// </summary>
public sealed class RecoverabilityBuilder
{
    private readonly RecoverabilityOptions _options;

    public RecoverabilityBuilder(RecoverabilityOptions options) => _options = options;

    public RecoverabilityBuilder ImmediateRetries(int count)
    {
        _options.ImmediateRetries = count;
        return this;
    }

    public RecoverabilityBuilder DelayedRetries(int count, TimeSpan? baseBackoff = null)
    {
        _options.DelayedRetries = count;
        if (baseBackoff is { } b)
            _options.DelayedBackoffBaseSeconds = b.TotalSeconds;
        return this;
    }

    public RecoverabilityBuilder DelayedBackoffMax(TimeSpan max)
    {
        _options.DelayedBackoffMaxSeconds = max.TotalSeconds;
        return this;
    }

    public RecoverabilityBuilder MapException<TException>(FailureAction action) where TException : Exception
    {
        _options.MapException<TException>(action);
        return this;
    }
}
```
