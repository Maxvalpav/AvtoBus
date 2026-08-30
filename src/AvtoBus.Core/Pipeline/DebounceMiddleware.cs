using System.Collections.Concurrent;
using AvtoBus.Configuration;

namespace AvtoBus.Pipeline;

/// <summary>
/// Сливает поток обновлений одного ключа в одно сообщение (идея 30).
/// Первое появление ключа откладывается; более новые заменяют его в буфере;
/// по истечении окна тишины доставляется только последнее.
/// </summary>
public sealed class DebounceMiddleware(BusOptions options) : IBusMiddleware
{
    private readonly ConcurrentDictionary<(string Queue, string Key), Guid> _latest = new();

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

        var mapKey = (context.Source.Name, keySelector(context.Message));

        // Повторная доставка после окна: доставляем, только если это всё ещё последнее
        // сообщение ключа. Иначе за время окна пришло новое — текущее уже неактуально.
        if (context.Envelope.DeliveryAttempt > 1)
        {
            if (_latest.TryGetValue(mapKey, out var latestId) && latestId == context.Envelope.MessageId)
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

        // Первое появление ключа: возможно, следом придёт обновление. Заменяем буфер
        // и откладываем доставку — ретрай с задержкой вернёт сообщение после окна.
        _latest[mapKey] = context.Envelope.MessageId;
        await context.DeferAsync(window).ConfigureAwait(false);
    }
}
