using System.Collections.Concurrent;
using AvtoBus.Handlers;

namespace AvtoBus.Pipeline;

/// <summary>
/// Применяет <see cref="HandlerTimeoutAttribute"/> (идея 170): взводит связанный CancellationToken
/// на время обработки хендлера. Зависший хендлер прерывается, а не вешается навсегда.
/// </summary>
public sealed class TimeoutMiddleware(DispatcherRegistry dispatchers) : IBusMiddleware
{
    private readonly ConcurrentDictionary<Type, TimeSpan?> _cache = new();

    public async ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        var timeout = _cache.GetOrAdd(context.Message.GetType(), ResolveTimeout);
        if (timeout is null || timeout.Value <= TimeSpan.Zero)
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken);
        cts.CancelAfter(timeout.Value);

        var previous = context.CancellationToken;
        context.CancellationToken = cts.Token;

        try
        {
            await next(context).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested && !previous.IsCancellationRequested)
        {
            throw new HandlerTimeoutException(context.Envelope.MessageType, timeout.Value);
        }
        finally
        {
            context.CancellationToken = previous;
        }
    }

    /// <summary>
    /// Таймаут берётся с метода или класса хендлера через <see cref="IHandlerTimeoutProvider"/>.
    /// Для типов без <see cref="HandlerTimeoutAttribute"/> — null: обрабатываем без ограничения.
    /// </summary>
    private TimeSpan? ResolveTimeout(Type messageType)
    {
        foreach (var dispatcher in dispatchers.For(messageType))
        {
            if (dispatcher is IHandlerTimeoutProvider { Timeout: { } timeout })
                return timeout;
        }

        return null;
    }
}

/// <summary>Хендлер превысил [HandlerTimeout]: обработка прервана токеном (идея 170).</summary>
public sealed class HandlerTimeoutException(string messageType, TimeSpan timeout)
    : Exception($"Handler for '{messageType}' exceeded timeout {timeout}");
