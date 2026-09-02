using System.Collections.Concurrent;

namespace AvtoBus.Runtime;

/// <summary>
/// Сопоставляет ответы с ожидающими их запросами. Локальные вызовы завершаются
/// напрямую через <see cref="TaskCompletionSource"/> — микросекунды вместо похода в брокер (идея 48).
/// </summary>
public sealed class ReplyRouter
{
    private readonly ConcurrentDictionary<Guid, PendingRequest> _pending = new();

    /// <summary>Имя очереди ответов этого процесса. Уникально на инстанс.</summary>
    public string ReplyAddress { get; } = $"reply-{Guid.NewGuid():N}";

    /// <summary>Регистрирует ожидание ответа на запрос.</summary>
    public Task<object> RegisterAsync(Guid requestId, Type replyType, TimeSpan timeout, CancellationToken ct)
    {
        var completion = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = new PendingRequest(replyType, completion);

        if (!_pending.TryAdd(requestId, pending))
            throw new InvalidOperationException($"Запрос {requestId} уже ожидает ответа.");

        return AwaitAsync(requestId, pending, timeout, ct);
    }

    private async Task<object> AwaitAsync(
        Guid requestId,
        PendingRequest pending,
        TimeSpan timeout,
        CancellationToken ct)
    {
        using var timeoutSource = new CancellationTokenSource(timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutSource.Token);

        try
        {
            return await pending.Completion.Task.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutSource.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            if (!pending.Completion.Task.IsCompleted) pending.Completion.TrySetCanceled();
            throw new TimeoutException(
                $"Ответ на запрос {requestId} не получен за {timeout.TotalSeconds:0.##} с.");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            if (!pending.Completion.Task.IsCompleted) pending.Completion.TrySetCanceled(ct);
            throw;
        }
        finally
        {
            _pending.TryRemove(requestId, out _);
        }
    }

    /// <summary>
    /// Доставляет ответ ожидающему. Возвращает <c>false</c>, если никто не ждёт:
    /// запрос мог истечь по таймауту — это нормально, не ошибка.
    /// </summary>
    public bool TryComplete(Guid requestId, object reply)
    {
        if (!_pending.TryRemove(requestId, out var pending))
            return false;

        if (!pending.ReplyType.IsInstanceOfType(reply))
        {
            pending.Completion.TrySetException(new InvalidOperationException(
                $"Ожидался ответ типа {pending.ReplyType.Name}, получен {reply.GetType().Name}."));
            return true;
        }

        return pending.Completion.TrySetResult(reply);
    }

    /// <summary>Проваливает ожидание: обработчик запроса упал.</summary>
    public bool TryFail(Guid requestId, Exception exception)
        => _pending.TryRemove(requestId, out var pending)
           && pending.Completion.TrySetException(exception);

    /// <summary>Тип ожидаемого ответа — нужен приёмнику для десериализации. TryComplete — атомарно.</summary>
    public bool IsAwaiting(Guid requestId) => _pending.ContainsKey(requestId);

    public bool TryGetReplyType(Guid requestId, out Type? replyType)
    {
        if (_pending.TryGetValue(requestId, out var p)) { replyType = p.ReplyType; return true; }
        replyType = null; return false;
    }

    /// <summary>Число активных ожиданий ответа. После таймаута/отмены должно возвращаться к 0.</summary>
    public int PendingCount => _pending.Count;

    private readonly record struct PendingRequest(Type ReplyType, TaskCompletionSource<object> Completion);
}
