using System.Collections.Concurrent;
using AvtoBus.Pipeline;

namespace AvtoBus.Testing;

/// <summary>
/// Записывает всё, что проходит через шину: что обработано, что опубликовано, что упало.
/// На этом строятся утверждения тестов.
/// </summary>
public sealed class BusRecorder
{
    private readonly ConcurrentQueue<RecordedMessage> _consumed = new();
    private readonly ConcurrentQueue<RecordedMessage> _published = new();
    private readonly ConcurrentQueue<RecordedFault> _faults = new();
    private readonly ConcurrentQueue<RecordedMessage> _deadLettered = new();

    public IReadOnlyCollection<RecordedMessage> Consumed => _consumed;

    public IReadOnlyCollection<RecordedMessage> Published => _published;

    public IReadOnlyCollection<RecordedFault> Faults => _faults;

    public IReadOnlyCollection<RecordedMessage> DeadLettered => _deadLettered;

    internal void RecordConsumed(object message, Envelope envelope)
        => _consumed.Enqueue(new RecordedMessage(message, envelope));

    internal void RecordPublished(object message, Envelope envelope)
        => _published.Enqueue(new RecordedMessage(message, envelope));

    internal void RecordFault(object? message, Envelope envelope, Exception exception)
        => _faults.Enqueue(new RecordedFault(message, envelope, exception));

    internal void RecordDeadLettered(object? message, Envelope envelope)
        => _deadLettered.Enqueue(new RecordedMessage(message, envelope));

    /// <summary>Все обработанные сообщения указанного типа.</summary>
    public IEnumerable<T> ConsumedOf<T>() where T : class
        => _consumed.Select(m => m.Message).OfType<T>();

    /// <summary>Все опубликованные сообщения указанного типа.</summary>
    public IEnumerable<T> PublishedOf<T>() where T : class
        => _published.Select(m => m.Message).OfType<T>();

    public int CountConsumed<T>() where T : class => ConsumedOf<T>().Count();

    public void Clear()
    {
        _consumed.Clear();
        _published.Clear();
        _faults.Clear();
        _deadLettered.Clear();
    }
}

public sealed record RecordedMessage(object? Message, Envelope Envelope);

public sealed record RecordedFault(object? Message, Envelope Envelope, Exception Exception);

/// <summary>
/// Middleware, наполняющий <see cref="BusRecorder"/> и умеющий инжектить сбои (идея 325).
/// </summary>
public sealed class RecordingMiddleware(BusRecorder recorder, FaultInjector faults) : IBusMiddleware
{
    public async ValueTask InvokeAsync(ConsumeContext context, BusDelegate next)
    {
        recorder.RecordConsumed(context.Message, context.Envelope);

        if (faults.ShouldFail(context.Message.GetType(), out var exception))
        {
            recorder.RecordFault(context.Message, context.Envelope, exception);
            throw exception;
        }

        if (faults.DelayOf(context.Message.GetType()) is { } delay && delay > TimeSpan.Zero)
            await Task.Delay(delay, context.CancellationToken).ConfigureAwait(false);

        try
        {
            await next(context).ConfigureAwait(false);
        }
        catch (Exception thrown)
        {
            recorder.RecordFault(context.Message, context.Envelope, thrown);
            throw;
        }

        if (context.Outcome is ConsumeOutcome.DeadLettered)
            recorder.RecordDeadLettered(context.Message, context.Envelope);

        foreach (var outgoing in context.Outgoing)
            recorder.RecordPublished(outgoing.Message, context.Envelope);
    }
}

/// <summary>
/// Управляемая инжекция сбоев: «уронить следующие N сообщений типа T» (идея 325).
/// </summary>
public sealed class FaultInjector
{
    private readonly ConcurrentDictionary<Type, FaultPlan> _plans = new();
    private readonly ConcurrentDictionary<Type, TimeSpan> _delays = new();

    /// <summary>Следующие <paramref name="times"/> сообщений типа <typeparamref name="T"/> упадут.</summary>
    public FaultInjector FailNext<T>(int times = 1, Exception? exception = null)
    {
        _plans[typeof(T)] = new FaultPlan(
            times,
            exception ?? new InvalidOperationException($"Инжектированный сбой для {typeof(T).Name}"));
        return this;
    }

    /// <summary>Задерживает обработку сообщений типа <typeparamref name="T"/> — для проверки таймаутов.</summary>
    public FaultInjector DelayNext<T>(TimeSpan delay)
    {
        _delays[typeof(T)] = delay;
        return this;
    }

    public void Clear()
    {
        _plans.Clear();
        _delays.Clear();
    }

    internal bool ShouldFail(Type messageType, out Exception exception)
    {
        exception = null!;

        if (!_plans.TryGetValue(messageType, out var plan))
            return false;

        var remaining = plan.Decrement();
        if (remaining < 0)
        {
            _plans.TryRemove(messageType, out _);
            return false;
        }

        exception = plan.Exception;
        return true;
    }

    internal TimeSpan? DelayOf(Type messageType)
        => _delays.TryGetValue(messageType, out var delay) ? delay : null;

    private sealed class FaultPlan(int times, Exception exception)
    {
        private int _remaining = times;

        public Exception Exception { get; } = exception;

        /// <summary>Возвращает число оставшихся сбоев; отрицательное значение — план исчерпан.</summary>
        public int Decrement() => Interlocked.Decrement(ref _remaining);
    }
}
