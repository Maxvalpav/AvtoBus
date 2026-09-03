using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using AvtoBus;

namespace AvtoBus.Runtime;

/// <summary>
/// Request Streaming как server-streaming: `IBus.RequestStreamAsync` шлет 1 запрос → N ответов `IAsyncEnumerable`. Завершение — `StreamEnd` marker.
/// Хендлер стримит через `ctx.StreamAsync(reply)` / `ctx.CompleteStream()`.
/// </summary>
public interface IBusStreaming
{
    IAsyncEnumerable<TReply> RequestStreamAsync<TRequest, TReply>(TRequest request, TimeSpan? timeout = null, CancellationToken ct = default)
        where TRequest : class where TReply : class;
}

public sealed class StreamingReplyRouter
{
    private readonly ConcurrentDictionary<Guid, StreamingChannel> _channels = new();
    public string ReplyAddress { get; } = $"reply-stream-{Guid.NewGuid():N}";
    public IAsyncEnumerable<TReply> RegisterStream<TReply>(Guid requestId, TimeSpan timeout, CancellationToken ct) where TReply : class
    {
        var ch = System.Threading.Channels.Channel.CreateUnbounded<object>();
        _channels[requestId] = new StreamingChannel(ch, typeof(TReply));
        return ReadAsync<TReply>(requestId, ch, timeout, ct);
    }

    private async IAsyncEnumerable<TReply> ReadAsync<TReply>(Guid id, System.Threading.Channels.Channel<object> ch, TimeSpan timeout, [EnumeratorCancellation] CancellationToken ct) where TReply : class
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try
        {
            await foreach (var obj in ch.Reader.ReadAllAsync(cts.Token))
            {
                if (obj is StreamEnd) break;
                if (obj is Exception ex) throw ex;
                yield return (TReply)obj;
            }
        }
        finally
        {
            // Таймаут/отмена раньше бросали исключение до TryRemove — канал и запись
            // в словаре текли навсегда. Чистим всегда и будим возможных продюсеров.
            _channels.TryRemove(id, out _);
            try { ch.Writer.TryComplete(); } catch { }
        }
    }

    public bool TryPush(Guid requestId, object reply)
    {
        if (!_channels.TryGetValue(requestId, out var ch)) return false;
        if (!ch.ReplyType.IsInstanceOfType(reply)) return false;
        return ch.Channel.Writer.TryWrite(reply);
    }
    public bool TryComplete(Guid requestId)
    {
        if (!_channels.TryGetValue(requestId, out var ch)) return false;
        if (!ch.Channel.Writer.TryWrite(new StreamEnd())) return false;
        ch.Channel.Writer.TryComplete();
        _channels.TryRemove(requestId, out _);
        return true;
    }
    public bool TryFail(Guid requestId, Exception ex)
    {
        if (!_channels.TryGetValue(requestId, out var ch)) return false;
        ch.Channel.Writer.TryWrite(ex);
        ch.Channel.Writer.TryComplete();
        _channels.TryRemove(requestId, out _);
        return true;
    }
    public bool IsStreaming(Guid id) => _channels.ContainsKey(id);
    private sealed record StreamingChannel(System.Threading.Channels.Channel<object> Channel, Type ReplyType);
    private sealed record StreamEnd;
}

public static class StreamConsumeContextExtensions
{
    public static ValueTask StreamAsync<T>(this ConsumeContext ctx, T reply) where T : class
    {
        // Стримит один chunk как Respond но с header `avtobus.stream=chunk`
        var opts = new SendOptions().WithHeader("avtobus.stream", "chunk");
        ctx.Enqueue(new OutgoingMessage(reply, OutgoingKind.Respond, opts));
        return ValueTask.CompletedTask;
    }
    public static void CompleteStream(this ConsumeContext ctx)
    {
        ctx.Enqueue(new OutgoingMessage(new StreamEndMarker(), OutgoingKind.Respond, new SendOptions().WithHeader("avtobus.stream", "end")));
    }
    private sealed class StreamEndMarker;
}

public static class RequestStreamingExtensions
{
    public static IAsyncEnumerable<TReply> RequestStreamAsync<TRequest, TReply>(this IBus bus, TRequest request, StreamingReplyRouter router, TimeSpan? timeout = null, CancellationToken ct = default)
        where TRequest : class where TReply : class
    {
        // Стаб: реальный транспорт — как RequestAsync но через streaming router
        var id = Guid.NewGuid();
        return router.RegisterStream<TReply>(id, timeout ?? TimeSpan.FromSeconds(30), ct);
    }
}
