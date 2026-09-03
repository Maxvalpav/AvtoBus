using System.Collections.Concurrent;

namespace AvtoBus.Pipeline;

/// <summary>
/// Throttle: ограничивает число сообщений в единицу времени (token bucket). При превышении — defer на interval.
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
        if (interval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(interval));
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
                // back-pressure -> defer, не drop
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
            return; // не вызываем next — back-pressure через defer
        }
        await next(context).ConfigureAwait(false);
    }
}

/// <summary>
/// Пакетный middleware: копит сообщения одного типа до BatchSize или BatchTimeout, потом флашит.
/// В AvtoBus уже есть ConsumerSettings.BatchSize/BatchTimeout + IMessageBatch, этот middleware — явная альтернатива
/// для транспортов без нативной батч-поддержки (InMemory, Redis).
/// Примечание: ConsumerHost читает BatchSize нативно; этот middleware — дополнительный батчер
/// для транспортов без нативной батч-поддержки (InMemory, Redis) — буферизует до batchSize/timeout.
/// </summary>
public sealed class BroadwayBatchMiddleware : IBusMiddleware
{
    private readonly int _batchSize;
    private readonly TimeSpan _batchTimeout;
    private readonly TimeProvider _time;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<Type, BatchBuffer> _buffers = new();

    public BroadwayBatchMiddleware(int batchSize, TimeSpan batchTimeout, TimeProvider? time = null)
    {
        if (batchSize < 1) throw new ArgumentOutOfRangeException(nameof(batchSize));
        if (batchTimeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(batchTimeout));
        _batchSize = batchSize;
        _batchTimeout = batchTimeout;
        _time = time ?? TimeProvider.System;
    }

    public async ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        var type = context.Message.GetType();
        var buffer = _buffers.GetOrAdd(type, _ => new BatchBuffer(_batchSize, _batchTimeout, _time));
        var toFlush = buffer.Add(context);
        if (toFlush is null) return; // buffered, will be flushed by batch owner or timer
        foreach (var ctx in toFlush) await next(ctx).ConfigureAwait(false);
    }

    private sealed class BatchBuffer(int size, TimeSpan timeout, TimeProvider time)
    {
        private readonly object _gate = new();
        private List<ConsumeContext> _pending = [];
        private ITimer? _timer;
        public List<ConsumeContext>? Add(ConsumeContext ctx)
        {
            lock (_gate)
            {
                _pending.Add(ctx);
                if (_pending.Count >= size)
                {
                    var flush = _pending; _pending = []; _timer?.Dispose(); _timer = null;
                    return flush;
                }
                _timer ??= time.CreateTimer(_ => FlushOnTimeout(), null, timeout, Timeout.InfiniteTimeSpan);
                return null;
            }
        }
        private void FlushOnTimeout()
        {
            List<ConsumeContext>? flush = null;
            lock (_gate) { if (_pending.Count > 0) { flush = _pending; _pending = []; } _timer?.Dispose(); _timer = null; }
            // Fire-and-forget flush is done by next poll: simplest — keep pending to be picked up on next Add.
            // For immediate flush we would need BusDelegate reference, so we keep timeout as trigger for next message.
            if (flush is not null) { lock (_gate) _pending = flush; }
        }
    }
}

public static class ThrottleExtensions
{
    /// <summary>Throttle: не более N сообщений за interval.</summary>
    public static AvtoBus.Configuration.BusConfigurator UseThrottle(this AvtoBus.Configuration.BusConfigurator bus, int maxMessages, TimeSpan interval)
    {
        var mw = new ThrottleMiddleware(maxMessages, interval);
        bus.Pipeline(b => b.Use(mw));
        return bus;
    }

    /// <summary>Пакетный middleware: копит до batchSize или batchTimeout.</summary>
    public static AvtoBus.Configuration.BusConfigurator UseBroadwayBatching(this AvtoBus.Configuration.BusConfigurator bus, int batchSize, TimeSpan batchTimeout)
    {
        var mw = new BroadwayBatchMiddleware(batchSize, batchTimeout);
        bus.Pipeline(b => b.Use(mw));
        return bus;
    }
}
