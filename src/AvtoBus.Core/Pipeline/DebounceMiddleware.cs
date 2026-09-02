using System.Collections.Concurrent;
using AvtoBus.Configuration;

namespace AvtoBus.Pipeline;

/// <summary>
/// Сливает поток обновлений одного ключа в одно сообщение (идея 30).
/// Первое появление ключа откладывается; более новые заменяют его в буфере;
/// по истечении окна тишины доставляется только последнее.
/// </summary>
public sealed class DebounceMiddleware(BusOptions options, TimeProvider? timeProvider = null) : IBusMiddleware
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private readonly ConcurrentDictionary<(string Queue, string Key), (Guid Id, DateTimeOffset ExpiresAt)> _latest = new();

    public async ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        var type = context.Message.GetType();
        if (!options.Consumers.TryGetValue(type, out var settings)
            || settings.DebounceWindow is not { } window
            || settings.DebounceKeySelector is not { } keySelector)
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        // Evict expired entries to bound memory for high-cardinality keys
        EvictExpired();

        var mapKey = (context.Source.Name, keySelector(context.Message));

        if (context.Envelope.DeliveryAttempt > 1)
        {
            if (_latest.TryGetValue(mapKey, out var entry) && entry.Id == context.Envelope.MessageId)
            {
                _latest.TryRemove(mapKey, out _);
                await next(context).ConfigureAwait(false);
            }
            else
            {
                context.Skip("debounce: superseded");
            }
            return;
        }

        _latest[mapKey] = (context.Envelope.MessageId, _time.GetUtcNow() + window + TimeSpan.FromMinutes(1));
        await context.DeferAsync(window).ConfigureAwait(false);
    }

    private void EvictExpired()
    {
        var now = _time.GetUtcNow();
        foreach (var (k, v) in _latest)
            if (now >= v.ExpiresAt) _latest.TryRemove(k, out _);
    }
}
