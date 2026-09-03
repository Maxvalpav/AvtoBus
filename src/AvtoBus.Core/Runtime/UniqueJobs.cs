using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Runtime;

/// <summary>
/// Уникальная задача: предотвращает повторную постановку одинакового сообщения,
/// пока предыдущее ещё в очереди / в окне уникальности.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = true)]
public sealed class UniqueJobAttribute : Attribute
{
    /// <summary>Окно уникальности. По умолчанию 30 секунд.</summary>
    public int PeriodSeconds { get; init; } = 30;

    public TimeSpan Period => TimeSpan.FromSeconds(PeriodSeconds);

    /// <summary>Учитывать очередь/топик в ключе уникальности.</summary>
    public bool ByQueue { get; init; } = true;

    /// <summary>Учитывать тело сообщения (args). false = только тип+очередь.</summary>
    public bool ByArgs { get; init; } = true;

    /// <summary>Префикс ключа. Если не задан — тип сообщения.</summary>
    public string? KeyPrefix { get; init; }

    /// <summary>Что делать при конфликте: молча пропустить (Oban) или кинуть исключение.</summary>
    public UniqueConflictBehavior OnConflict { get; init; } = UniqueConflictBehavior.Skip;
}

public enum UniqueConflictBehavior
{
    /// <summary>Не отправлять дубликат, вернуть false. Как Oban `{:cancel, :duplicate}`.</summary>
    Skip,
    /// <summary>Кинуть <see cref="DuplicateMessageException"/>.</summary>
    Throw
}

public sealed class DuplicateMessageException(string key, string messageType)
    : InvalidOperationException($"Дубликат {messageType} с ключом {key} уже в очереди (UniqueJob).")
{
    public string Key { get; } = key;
    public string MessageType { get; } = messageType;
}

public interface IUniqueStore
{
    /// <summary>Пытается занять слот. true = занято (можно отправлять), false = уже занят.</summary>
    bool TryAcquire(string key, TimeSpan ttl);
    void Release(string key);
    bool IsHeld(string key);
}

public sealed class InMemoryUniqueStore(TimeProvider? time = null) : IUniqueStore, IDisposable
{
    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private readonly ConcurrentDictionary<string, DateTimeOffset> _slots = new();

    public bool TryAcquire(string key, TimeSpan ttl)
    {
        var now = _time.GetUtcNow();
        Cleanup(now);
        var expires = now + ttl;
        // TryAdd, но если уже есть и истёк — заменить
        while (true)
        {
            if (_slots.TryAdd(key, expires)) return true;
            if (_slots.TryGetValue(key, out var existing))
            {
                if (existing <= now)
                {
                    if (_slots.TryUpdate(key, expires, existing)) return true;
                    continue; // race, retry
                }
                return false; // still held
            }
        }
    }

    public void Release(string key) => _slots.TryRemove(key, out _);
    public bool IsHeld(string key)
    {
        if (!_slots.TryGetValue(key, out var exp)) return false;
        if (exp <= _time.GetUtcNow()) { _slots.TryRemove(key, out _); return false; }
        return true;
    }

    private long _cleanupCounter;
    private void Cleanup(DateTimeOffset now)
    {
        // Throttle cleanup: every 100 acquires to avoid O(N) scan on hot path
        if (Interlocked.Increment(ref _cleanupCounter) % 100 != 0 && _slots.Count < 10000) return;
        foreach (var kv in _slots)
            if (kv.Value <= now) _slots.TryRemove(kv.Key, out _);
    }

    public void Dispose() { }
}

public static class UniqueKeyComputer
{
    // Один инстанс опций на процесс: new JsonSerializerOptions на каждое сообщение —
    // дорогая аллокация (кэши метаданных сериализатора) на send-пути.
    private static readonly JsonSerializerOptions StableOptions = new() { WriteIndented = false };

    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "Хэш тела через reflection-STJ произвольного типа — несовместимо с trimming/AOT.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Хэш тела через reflection-STJ произвольного типа — несовместимо с NativeAOT.")]
    public static string Compute<T>(T message, Type messageType, string destination, UniqueJobAttribute attr)
    {
        var prefix = attr.KeyPrefix ?? messageType.FullName ?? messageType.Name;
        if (!attr.ByArgs) return $"{prefix}::{destination}";
        // хэш тела: stable json hash
        var json = JsonSerializer.Serialize(message, messageType, StableOptions);
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(json)))[..16];
        return attr.ByQueue ? $"{prefix}::{destination}::{hash}" : $"{prefix}::{hash}";
    }

    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "Хэш тела через reflection-STJ произвольного типа — несовместимо с trimming/AOT.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Хэш тела через reflection-STJ произвольного типа — несовместимо с NativeAOT.")]
    public static string Compute(object message, Type messageType, string destination, UniqueJobAttribute attr)
        => Compute(message, messageType, destination, attr);
}

/// <summary>Проверяет уникальность перед отправкой. Вставляется в AvtoBusClient.Prepare (producer-side).</summary>
public static class UniqueJobExtensions
{
    public static BusConfigurator UseUniqueJobs(this BusConfigurator bus, TimeProvider? time = null)
    {
        bus.Services.AddSingleton<IUniqueStore>(new InMemoryUniqueStore(time));
        return bus;
    }

    public static BusConfigurator UseUniqueJobs<
        [System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicConstructors)] TStore>(
        this BusConfigurator bus)
        where TStore : class, IUniqueStore
    {
        bus.Services.AddSingleton<IUniqueStore, TStore>();
        return bus;
    }

    /// <summary>Явный ключ уникальности per-message для дедупликации.</summary>
    public static SendOptions WithUniqueKey(this SendOptions opts, string key, TimeSpan? period = null)
    {
        opts.WithHeader("avtobus.unique-key", key);
        if (period is not null) opts.WithHeader("avtobus.unique-ttl", period.Value.TotalMilliseconds.ToString("F0", System.Globalization.CultureInfo.InvariantCulture));
        return opts;
    }

    public static PublishOptions WithUniqueKey(this PublishOptions opts, string key, TimeSpan? period = null)
    {
        opts.WithHeader("avtobus.unique-key", key);
        if (period is not null) opts.WithHeader("avtobus.unique-ttl", period.Value.TotalMilliseconds.ToString("F0", System.Globalization.CultureInfo.InvariantCulture));
        return opts;
    }
}

/// <summary>Middleware потребительского side: если сообщение помечено unique и уже обрабатывается — skip (идемпотентность как у Oban).
/// Но основная защита — producer-side в AvtoBusClient.</summary>
public sealed class UniqueJobConsumerMiddleware : AvtoBus.Pipeline.IBusMiddleware
{
    private readonly IUniqueStore _store;
    public UniqueJobConsumerMiddleware(IUniqueStore store) => _store = store;
    public ValueTask InvokeAsync(ConsumeContext context, AvtoBus.Pipeline.BusDelegate next)
    {
        if (context.Envelope.Headers.TryGetValue("avtobus.unique-key", out var key) && !string.IsNullOrWhiteSpace(key))
        {
            if (_store.IsHeld(key))
            {
                context.Skip($"unique-held:{key}");
                return ValueTask.CompletedTask;
            }
            var ttl = TimeSpan.FromSeconds(30);
            if (context.Envelope.Headers.TryGetValue("avtobus.unique-ttl", out var ttlStr) && double.TryParse(ttlStr, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var ms) && double.IsFinite(ms) && ms > 0)
                ttl = TimeSpan.FromMilliseconds(ms);
            _store.TryAcquire(key, ttl);
        }
        return next(context);
    }
}
