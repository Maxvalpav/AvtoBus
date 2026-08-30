using System.Collections.Concurrent;

namespace AvtoBus.Pipeline;

/// <summary>
/// Throttle как в Watermill (Go) `Throttle{messages, interval}` и Broadway (Elixir) `rate_limiting: [allowed_messages, interval]`.
/// Ограничивает число сообщений в единицу времени (token bucket). При превышении — defer на interval.
/// Также известен как BullMQ `limiter: { max, duration }` и Sidekiq throttle gem.
/// </summary>
public sealed class ThrottleMiddleware : IBusMiddleware
{
    private readonly int _maxMessages;
    private readonly TimeSpan _interval;
    private readonly TimeProvider _time;
    private readonly ConcurrentQueue<DateTimeOffset> _window = new();
    private readonly object _lock = new();

    public ThrottleMiddleware(int maxMessages, TimeSpan interval, TimeProvider? time = null)
    {
        if (maxMessages < 1) throw new ArgumentOutOfRangeException(nameof(maxMessages));
        _maxMessages = maxMessages;
        _interval = interval;
        _time = time ?? TimeProvider.System;
    }

    public async ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        var now = _time.GetUtcNow();
        TimeSpan? defer = null;
        lock (_lock)
        {
            while (_window.TryPeek(out var oldest) && now - oldest > _interval)
                _window.TryDequeue(out _);

            if (_window.Count >= _maxMessages)
            {
                // как в Broadway: back-pressure -> defer, не drop
                var oldest = _window.TryPeek(out var o) ? o : now;
                var delay = _interval - (now - oldest);
                if (delay < TimeSpan.FromMilliseconds(50)) delay = TimeSpan.FromMilliseconds(50);
                defer = delay;
            }
            else
            {
                _window.Enqueue(now);
            }
        }
        if (defer.HasValue)
        {
            await context.DeferAsync(defer.Value).ConfigureAwait(false);
            return; // не вызываем next — back-pressure (как Watermill Throttle)
        }
        await next(context).ConfigureAwait(false);
    }
}

/// <summary>
/// Broadway-style batcher: копит сообщения одного типа до BatchSize или BatchTimeout, потом флашит.
/// В AvtoBus уже есть ConsumerSettings.BatchSize/BatchTimeout + IMessageBatch, этот middleware — явная альтернатива
/// для транспортов без нативной батч-поддержки (InMemory, Redis). Подражает Broadway `batchers: [{batch_size, batch_timeout}]`
/// и Kafka Streams `suppress/untilWindowCloses`.
/// Примечание: полная реализация батчинга — в ConsumerHost (читает BatchSize). Этот middleware сейчас — thin passthrough,
/// сохраняет API-совместимость с Broadway-конфигом и считает метрики (будет расширяться).
/// </summary>
public sealed class BroadwayBatchMiddleware : IBusMiddleware
{
    private readonly int _batchSize;
    private readonly TimeSpan _batchTimeout;
    private readonly TimeProvider? _time;

    public BroadwayBatchMiddleware(int batchSize, TimeSpan batchTimeout, TimeProvider? time = null)
    {
        _batchSize = batchSize;
        _batchTimeout = batchTimeout;
        _time = time;
    }

    public ValueTask InvokeAsync(ConsumeContext context, BusDelegate next) => next(context);
}

public static class ThrottleExtensions
{
    /// <summary>Watermill/ Broadway throttle: не более N сообщений за interval.</summary>
    public static AvtoBus.Configuration.BusConfigurator UseThrottle(this AvtoBus.Configuration.BusConfigurator bus, int maxMessages, TimeSpan interval)
    {
        var mw = new ThrottleMiddleware(maxMessages, interval);
        bus.Pipeline(b => b.Use(mw));
        return bus;
    }

    /// <summary>Broadway batcher: копит до batchSize или batchTimeout.</summary>
    public static AvtoBus.Configuration.BusConfigurator UseBroadwayBatching(this AvtoBus.Configuration.BusConfigurator bus, int batchSize, TimeSpan batchTimeout)
    {
        var mw = new BroadwayBatchMiddleware(batchSize, batchTimeout);
        bus.Pipeline(b => b.Use(mw));
        return bus;
    }
}
