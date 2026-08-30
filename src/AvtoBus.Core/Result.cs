namespace AvtoBus;

/// <summary>
/// Различает бизнес-отказ и транзиентную ошибку (идея 50).
/// Бизнес-отказ не ретраится — это не сбой, а корректный исход.
/// </summary>
public enum ResultKind
{
    /// <summary>Успех; <see cref="Result{T}.Value"/> уходит каскадом.</summary>
    Ok,

    /// <summary>Бизнес-отказ: ретраи бессмысленны, сообщение отбрасывается или идёт в DLQ.</summary>
    Rejected,

    /// <summary>Временная проблема: имеет смысл повторить.</summary>
    Transient,
}

public readonly record struct Result
{
    private Result(ResultKind kind, string? reason)
    {
        Kind = kind;
        Reason = reason;
    }

    public ResultKind Kind { get; }

    public string? Reason { get; }

    public bool IsOk => Kind is ResultKind.Ok;

    public static Result Ok() => new(ResultKind.Ok, null);

    /// <summary>Бизнес-отказ: без ретраев.</summary>
    public static Result Reject(string reason) => new(ResultKind.Rejected, reason);

    /// <summary>Транзиентная ошибка: ретраить по политике recoverability.</summary>
    public static Result Transient(string reason) => new(ResultKind.Transient, reason);

    public static Result<T> Ok<T>(T value) => Result<T>.Ok(value);
}

/// <summary>Нетипизированный доступ к <see cref="Result{T}"/> без рефлексии (AOT-safe, идея 2).</summary>
public interface IResultValue
{
    ResultKind Kind { get; }

    string? Reason { get; }

    object? Value { get; }
}

/// <summary>Результат с полезной нагрузкой: при <see cref="ResultKind.Ok"/> значение публикуется каскадом.</summary>
public readonly record struct Result<T> : IResultValue
{
    private Result(ResultKind kind, T? value, string? reason)
    {
        Kind = kind;
        Value = value;
        Reason = reason;
    }

    public ResultKind Kind { get; }

    public T? Value { get; }

    public string? Reason { get; }

    object? IResultValue.Value => Value;

    public bool IsOk => Kind is ResultKind.Ok;

    public static Result<T> Ok(T value) => new(ResultKind.Ok, value, null);

    public static Result<T> Reject(string reason) => new(ResultKind.Rejected, default, reason);

    public static Result<T> Transient(string reason) => new(ResultKind.Transient, default, reason);

    public static implicit operator Result<T>(T value) => Ok(value);

    /// <summary>Отбрасывает значение, сохраняя исход — для единообразной обработки в пайплайне.</summary>
    public Result Untyped() => Kind switch
    {
        ResultKind.Ok => Result.Ok(),
        ResultKind.Rejected => Result.Reject(Reason!),
        _ => Result.Transient(Reason!),
    };
}

/// <summary>
/// Бизнес-отказ, выброшенный исключением. Recoverability не ретраит его — сразу финальное решение.
/// </summary>
public sealed class MessageRejectedException(string reason) : Exception($"Сообщение отклонено: {reason}")
{
    public string Reason { get; } = reason;
}
