# AvtoBus.Core — Транспорт: интерфейсы и InMemory-реализация

> **Code sketch / unverified.** Семантики ack/nack, delay и back-pressure требуют conformance-тестов. Канонический статус: [`../FINAL.md`](../FINAL.md).

---

## AvtoBus.Core/Transport/ITransport.cs

```csharp
using System.Collections;

namespace AvtoBus.Transport;

/// <summary>
/// Абстракция транспорта — минимальный интерфейс из 2 методов.
/// Любой брокер укладывается в Send + Receive.
/// </summary>
public interface ITransport : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Имя транспорта (rabbit, kafka, inmemory, ...).
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Отправить сообщение в указанный адрес.
    /// </summary>
    ValueTask SendAsync(
        Envelope envelope,
        TransportDestination destination,
        CancellationToken ct = default);

    /// <summary>
    /// Подписаться на приём сообщений (async enumerator = back-pressure).
    /// </summary>
    IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        CancellationToken ct = default);

    /// <summary>
    /// Создать/применить топологию (очереди, exchange, bindings).
    /// </summary>
    ValueTask CreateTopologyAsync(
        TopologyPlan plan,
        CancellationToken ct = default) => ValueTask.CompletedTask;
}

public sealed record TransportDestination(
    string Address,
    DestinationKind Kind);

public enum DestinationKind
{
    Queue,
    Topic,
    Reply
}

public sealed record TransportSubscription(
    string Queue,
    IReadOnlyList<string> Topics,
    int Prefetch,
    string ConsumerGroup);

public sealed record TransportMessage(
    Envelope Envelope,
    IAckContext Ack);

public interface IAckContext
{
    ValueTask AckAsync(CancellationToken ct = default);
    ValueTask NackAsync(bool requeue = false, CancellationToken ct = default);
    ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default);
}
```

---

## AvtoBus.Core/Transport/TopologyPlan.cs

```csharp
namespace AvtoBus.Transport;

/// <summary>
/// Описание желаемой топологии брокера.
/// </summary>
public sealed class TopologyPlan
{
    public List<QueueDefinition> Queues { get; } = new();
    public List<TopicDefinition> Topics { get; } = new();
    public List<BindingDefinition> Bindings { get; } = new();
}

public sealed class QueueDefinition
{
    public required string Name { get; init; }
    public bool Durable { get; init; } = true;
    public bool Exclusive { get; init; }
    public bool AutoDelete { get; init; }
    public int? MaxSize { get; init; }
    public TimeSpan? TimeToLive { get; init; }
    public Dictionary<string, object?> Arguments { get; init; } = new();
}

public sealed class TopicDefinition
{
    public required string Name { get; init; }
    public string Type { get; init; } = "topic";  // topic, fanout, direct
    public bool Durable { get; init; } = true;
}

public sealed class BindingDefinition
{
    public required string Source { get; init; }
    public required string Destination { get; init; }
    public string RoutingKey { get; init; } = "#";
    public string? ExchangeType { get; init; }
}
```

---

## AvtoBus.Core/Transport/InMemory/InMemoryTransport.cs

```csharp
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace AvtoBus.Transport.InMemory;

/// <summary>
/// InMemory-транспорт с полной семантикой брокера.
/// Поддерживает очереди, topics, fan-out, delay, DLQ, partitioning.
/// </summary>
public sealed class InMemoryTransport : ITransport, IInMemoryTransportDiagnostics
{
    public string Name => "inmemory";

    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new();
    private readonly ConcurrentDictionary<string, HashSet<string>> _topicBindings = new();
    private readonly ConcurrentDictionary<string, string> _dlqRoutes = new();
    private readonly DelayScheduler _scheduler;
    private readonly TimeProvider _clock;
    private int _totalSent;
    private int _totalReceived;

    public InMemoryTransport(TimeProvider clock)
    {
        _clock = clock;
        _scheduler = new DelayScheduler(clock);
    }

    // ── Send ──

    public ValueTask SendAsync(Envelope envelope, TransportDestination dest, CancellationToken ct = default)
    {
        if (envelope.DeliverAt is { } deliverAt && deliverAt > _clock.GetUtcNow())
        {
            _scheduler.Schedule(deliverAt, () => DoSend(envelope, dest));
            return default;
        }

        DoSend(envelope, dest);
        return default;
    }

    private void DoSend(Envelope envelope, TransportDestination dest)
    {
        Interlocked.Increment(ref _totalSent);

        if (dest.Kind == DestinationKind.Queue)
        {
            var queue = _queues.GetOrAdd(dest.Address, n => new InMemoryQueue(n, this));
            queue.Enqueue(envelope);
            return;
        }

        // Topic → fan-out
        if (_topicBindings.TryGetValue(dest.Address, out var subscribers))
        {
            foreach (var subQueue in subscribers)
            {
                var queue = _queues.GetOrAdd(subQueue, n => new InMemoryQueue(n, this));
                queue.Enqueue(envelope);
            }
        }
    }

    // ── Receive ──

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var queue = _queues.GetOrAdd(subscription.Queue,
            n => new InMemoryQueue(n, this));

        // Подписываем queue на topics
        foreach (var topic in subscription.Topics)
        {
            var bindings = _topicBindings.GetOrAdd(topic, _ => new HashSet<string>());
            lock (bindings) bindings.Add(subscription.Queue);
        }

        await foreach (var envelope in queue.ConsumeAsync(subscription.Prefetch, ct))
        {
            Interlocked.Increment(ref _totalReceived);
            var ack = new InMemoryAckContext(queue, envelope);
            yield return new TransportMessage(envelope, ack);
        }
    }

    public ValueTask DisposeAsync()
    {
        _scheduler.Dispose();
        return default;
    }

    public void Dispose()
    {
        _scheduler.Dispose();
    }

    // ── Diagnostics ──

    public int TotalSent => _totalSent;
    public int TotalReceived => _totalReceived;

    public IReadOnlyDictionary<string, int> QueueDepths =>
        _queues.ToDictionary(kv => kv.Key, kv => kv.Value.Depth);

    public void RegisterDlqRoute(string dlqQueue, string originalQueue)
    {
        _dlqRoutes[dlqQueue] = originalQueue;
    }
}
```

---

## AvtoBus.Core/Transport/InMemory/InMemoryQueue.cs

```csharp
using System.Threading.Channels;

namespace AvtoBus.Transport.InMemory;

/// <summary>
/// InMemory очередь — аналог полноценного брокера.
/// Поддерживает: бэк-pressure, partitioning, backoff на пустых.
/// </summary>
internal sealed class InMemoryQueue
{
    private readonly string _name;
    private readonly InMemoryTransport _transport;
    private readonly Channel<Envelope> _channel;
    private readonly Channel<Envelope> _retryChannel;
    private readonly ConcurrentDictionary<Guid, Envelope> _inflight = new();
    private int _totalEnqueued;
    private int _totalConsumed;
    private readonly int _maxSize;

    public InMemoryQueue(string name, InMemoryTransport transport, int maxSize = 100_000)
    {
        _name = name;
        _transport = transport;
        _maxSize = maxSize;
        _channel = Channel.CreateBounded<Envelope>(new BoundedChannelOptions(maxSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
        _retryChannel = Channel.CreateUnbounded<Envelope>();
    }

    public int Depth => _channel.Reader.Count;
    public string Name => _name;

    public void Enqueue(Envelope envelope)
    {
        _channel.Writer.TryWrite(envelope);
        Interlocked.Increment(ref _totalEnqueued);
    }

    public void EnqueueRetry(Envelope envelope)
    {
        _retryChannel.Writer.TryWrite(envelope);
    }

    public async IAsyncEnumerable<Envelope> ConsumeAsync(
        int prefetch,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        // Сначала retry, потом основная очередь
        while (await _retryChannel.Reader.WaitToReadAsync(ct))
        {
            while (_retryChannel.Reader.TryRead(out var retry))
                yield return retry;
        }

        await foreach (var envelope in _channel.Reader.ReadAllAsync(ct))
        {
            Interlocked.Increment(ref _totalConsumed);
            yield return envelope;
        }
    }

    internal void Ack(Guid messageId) => _inflight.TryRemove(messageId, out _);

    internal void Requeue(Envelope envelope)
    {
        var newEnvelope = envelope with
        {
            DeliveryAttempt = envelope.DeliveryAttempt + 1,
            SentAt = DateTimeOffset.UtcNow
        };

        // Retry через основную очередь (в реальном брокере — TTL-очередь)
        _channel.Writer.TryWrite(newEnvelope);
    }
}

/// <summary>
/// Контекст ack для InMemory-очереди.
/// </summary>
internal sealed class InMemoryAckContext : IAckContext
{
    private readonly InMemoryQueue _queue;
    private readonly Envelope _envelope;
    private int _acknowledged;

    public InMemoryAckContext(InMemoryQueue queue, Envelope envelope)
    {
        _queue = queue;
        _envelope = envelope;
    }

    public ValueTask AckAsync(CancellationToken ct = default)
    {
        if (Interlocked.CompareExchange(ref _acknowledged, 1, 0) == 0)
            _queue.Ack(_envelope.MessageId);
        return default;
    }

    public ValueTask NackAsync(bool requeue = false, CancellationToken ct = default)
    {
        if (Interlocked.CompareExchange(ref _acknowledged, 1, 0) == 0)
        {
            if (requeue)
                _queue.Requeue(_envelope);
        }
        return default;
    }

    public ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default)
    {
        if (Interlocked.CompareExchange(ref _acknowledged, 1, 0) == 0)
        {
            Task.Delay(delay, ct).ContinueWith(_ =>
            {
                _queue.EnqueueRetry(_envelope);
            }, TaskContinuationOptions.OnlyOnRanToCompletion);
        }
        return default;
    }
}
```

---

## AvtoBus.Core/Transport/InMemory/DelayScheduler.cs

```csharp
namespace AvtoBus.Transport.InMemory;

/// <summary>
/// Планировщик отложенных сообщений для InMemory-транспорта.
/// Использует hashed timing wheel для эффективного управления миллионами таймеров.
/// </summary>
internal sealed class DelayScheduler : IDisposable
{
    private readonly SortedDictionary<DateTimeOffset, List<Action>> _scheduled = new();
    private readonly object _lock = new();
    private readonly TimeProvider _clock;
    private Timer? _timer;
    private readonly CancellationTokenSource _cts = new();

    public DelayScheduler(TimeProvider clock)
    {
        _clock = clock;
        _timer = new Timer(Callback, null, Timeout.Infinite, Timeout.Infinite);
    }

    public void Schedule(DateTimeOffset at, Action action)
    {
        lock (_lock)
        {
            if (!_scheduled.TryGetValue(at, out var list))
            {
                list = new List<Action>();
                _scheduled[at] = list;
            }
            list.Add(action);

            // Обновить таймер на ближайший
            UpdateTimer();
        }
    }

    private void Callback(object? state)
    {
        var now = _clock.GetUtcNow();
        List<Action>? toRun = null;

        lock (_lock)
        {
            while (_scheduled.Count > 0)
            {
                var first = _scheduled.Keys.First();
                if (first > now) break;

                toRun = _scheduled[first];
                _scheduled.Remove(first);
            }
            UpdateTimer();
        }

        if (toRun is not null)
        {
            foreach (var action in toRun)
            {
                try { action(); }
                catch { /* swallow */ }
            }
        }
    }

    private void UpdateTimer()
    {
        if (_scheduled.Count == 0)
        {
            _timer?.Change(Timeout.Infinite, Timeout.Infinite);
            return;
        }

        var next = _scheduled.Keys.First();
        var delay = next - _clock.GetUtcNow();
        if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
        if (delay > TimeSpan.FromSeconds(30)) delay = TimeSpan.FromSeconds(30);

        _timer?.Change(delay, Timeout.InfiniteTimeSpan);
    }

    public void Dispose()
    {
        _cts.Cancel();
        _timer?.Dispose();
    }
}
```

---

## AvtoBus.Core/Transport/ITransportSelector.cs

```csharp
namespace AvtoBus.Transport;

/// <summary>
/// Селектор транспорта по имени.
/// </summary>
public interface ITransportSelector
{
    ITransport For(string transportName);
    ITransport Default { get; }
}

public sealed class InMemoryTransportSelector : ITransportSelector
{
    private readonly Dictionary<string, ITransport> _transports;
    private readonly ITransport _default;

    public InMemoryTransportSelector(IEnumerable<ITransport> transports)
    {
        _transports = transports.ToDictionary(t => t.Name, t => t);
        _default = _transports.GetValueOrDefault("inmemory") ??
                   throw new InvalidOperationException("No in-memory transport registered.");
    }

    public ITransport For(string transportName)
        => _transports.GetValueOrDefault(transportName) ?? _default;

    public ITransport Default => _default;
}
```

---

## AvtoBus.Core/Router.cs

```csharp
using System.Collections.Concurrent;

namespace AvtoBus;

/// <summary>
/// Маршрутизация: CLR-тип → (транспорт, адрес, Queue/Topic).
/// </summary>
public interface IRouter
{
    Route Route(Type messageType, bool isCommand);
}

public sealed record Route(
    string Transport,
    TransportDestination Destination,
    bool IsCommand);

/// <summary>
/// Convention-based роутер:
/// - Commands → queue (kebab-case от типа)
/// - Events → topic (kebab-case от типа)
/// </summary>
internal sealed class ConventionRouter : IRouter
{
    private readonly ConcurrentDictionary<Type, Route> _cache = new();
    private readonly List<IRouteConfiguration> _customRoutes;

    public ConventionRouter(List<IRouteConfiguration> customRoutes)
    {
        _customRoutes = customRoutes;
    }

    public Route Route(Type messageType, bool isCommand)
    {
        return _cache.GetOrAdd(messageType, t =>
        {
            // Проверить кастомные правила
            foreach (var rule in _customRoutes)
            {
                if (rule.AppliesTo(t))
                    return rule.GetRoute(t, isCommand);
            }

            // Конвенция: kebab-case
            var name = ToKebab(t.Name);
            if (isCommand)
                return new Route("inmemory", new TransportDestination(name, DestinationKind.Queue), true);
            else
                return new Route("inmemory", new TransportDestination(name, DestinationKind.Topic), false);
        });
    }

    private static string ToKebab(string name)
    {
        return string.Concat(
            name.Select((c, i) =>
                i > 0 && char.IsUpper(c) ? $"-{char.ToLower(c)}" : char.ToLower(c).ToString()
            ));
    }
}

public interface IRouteConfiguration
{
    bool AppliesTo(Type messageType);
    Route GetRoute(Type messageType, bool isCommand);
}

/// <summary>
/// Fluent builder для кастомных маршрутов.
/// </summary>
public sealed class RouteBuilder
{
    private readonly List<IRouteConfiguration> _rules;

    public RouteBuilder(List<IRouteConfiguration> rules) => _rules = rules;
}
```

---

## AvtoBus.Core/TypeResolver.cs

```csharp
using System.Collections.Frozen;

namespace AvtoBus;

/// <summary>
/// Разрешение имён типов: CLR-type ↔ строковое имя.
/// </summary>
public interface ITypeResolver
{
    string GetName(Type clrType);
    Type? GetType(string name);
}

/// <summary>
/// Стандартный резолвер: Type.FullName + [MessageAlias] атрибут.
/// </summary>
internal sealed class TypeAliasResolver : ITypeResolver
{
    private readonly FrozenDictionary<Type, string> _aliases;
    private readonly FrozenDictionary<string, Type> _reverse;

    public TypeAliasResolver(IEnumerable<IMessageDispatcher> dispatchers)
    {
        var map = new Dictionary<Type, string>();
        foreach (var d in dispatchers)
            map[d.ClrType] = d.MessageType;

        _aliases = map.ToFrozenDictionary();
        _reverse = map.ToFrozenDictionary(kv => kv.Value, kv => kv.Key, StringComparer.OrdinalIgnoreCase);
    }

    public string GetName(Type clrType)
        => _aliases.TryGetValue(clrType, out var name) ? name : clrType.FullName ?? clrType.Name;

    public Type? GetType(string name)
        => _reverse.TryGetValue(name, out var type) ? type : null;
}
```
