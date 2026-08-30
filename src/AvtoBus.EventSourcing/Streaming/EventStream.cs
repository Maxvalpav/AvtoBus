using System.Collections.Concurrent;

namespace AvtoBus.EventSourcing.Streaming;

/// <summary>Ключ группировки окна.</summary>
public sealed record GroupKey(object Value, string Display);

/// <summary>Одно окно агрегации с результатами по группам.</summary>
public sealed record WindowResult<TEvent>(
    long FromSequence,
    long ToSequence,
    DateTimeOffset OpenedAt,
    DateTimeOffset ClosedAt,
    IReadOnlyDictionary<string, GroupResult> Groups)
    where TEvent : class;

/// <summary>Агрегированная группа в окне.</summary>
public sealed record GroupResult(int Count, double? Sum, double? Average);

/// <summary>Стратегия окон: Tumbling фиксированной длины, Sliding с перекрытием.</summary>
public abstract record WindowStrategy(TimeSpan Size)
{
    /// <summary>Номер окна, в которое попадает событие (0-based с эпохи).</summary>
    public abstract long WindowIndex(DateTimeOffset timestamp);

    public static TumblingWindow Tumbling(TimeSpan size) => new(size);
    public static SlidingWindow Sliding(TimeSpan size, TimeSpan step) => new(size, step);
}

public sealed record TumblingWindow(TimeSpan Size) : WindowStrategy(Size)
{
    public override long WindowIndex(DateTimeOffset timestamp)
        => (long)(timestamp.UtcDateTime - DateTime.UnixEpoch).Ticks / Size.Ticks;
}

public sealed record SlidingWindow(TimeSpan Size, TimeSpan Step) : WindowStrategy(Size)
{
    public override long WindowIndex(DateTimeOffset timestamp)
        => (long)(timestamp.UtcDateTime - DateTime.UnixEpoch).Ticks / Step.Ticks;
}

/// <summary>Готовый пайплайн: окно + группировка + агрегат + потребитель.</summary>
public sealed class EventStream<TEvent> where TEvent : class
{
    private readonly IEventStore _store;
    private readonly IEventSerializer _serializer;
    private readonly WindowStrategy _window;
    private readonly Func<TEvent, string> _keySelector;
    private readonly Func<TEvent, string> _keyDisplay;
    private readonly Func<IReadOnlyList<TEvent>, double?> _sum;
    private readonly Func<WindowResult<TEvent>, ValueTask> _into;
    private long _fromSequence;

    internal EventStream(
        IEventStore store,
        IEventSerializer serializer,
        long fromSequence)
    {
        _store = store;
        _serializer = serializer;
        _fromSequence = fromSequence;
        _window = WindowStrategy.Tumbling(TimeSpan.FromHours(1));
        _keySelector = static _ => string.Empty;
        _keyDisplay = static _ => string.Empty;
        _sum = static _ => null;
        _into = static _ => ValueTask.CompletedTask;
    }

    private EventStream(EventStream<TEvent> source,
        WindowStrategy? window = null,
        Func<TEvent, string>? keySelector = null,
        Func<TEvent, string>? keyDisplay = null,
        Func<IReadOnlyList<TEvent>, double?>? sum = null,
        Func<WindowResult<TEvent>, ValueTask>? into = null)
    {
        _store = source._store;
        _serializer = source._serializer;
        _fromSequence = source._fromSequence;
        _window = window ?? source._window;
        _keySelector = keySelector ?? source._keySelector;
        _keyDisplay = keyDisplay ?? source._keyDisplay;
        _sum = sum ?? source._sum;
        _into = into ?? source._into;
    }

    /// <summary>Оконная стратегия (по умолчанию — Tumbling 1h).</summary>
    public EventStream<TEvent> Window(WindowStrategy window)
        => new(this, window: window);

    /// <summary>Группировка по ключу события.</summary>
    public EventStream<TEvent> GroupBy(Func<TEvent, string> key)
        => new(this, keySelector: key, keyDisplay: key);

    /// <summary>Группировка по ключу с отдельным отображаемым именем.</summary>
    public EventStream<TEvent> GroupBy(Func<TEvent, string> key, Func<TEvent, string> display)
        => new(this, keySelector: key, keyDisplay: display);

    /// <summary>Сумма числового поля по группе.</summary>
    public EventStream<TEvent> Aggregate(Func<IReadOnlyList<TEvent>, double> sum)
        => new(this, sum: list => (double?)sum(list));

    /// <summary>Куда доставлять результаты окон.</summary>
    public EventStream<TEvent> Into(Action<WindowResult<TEvent>> into)
        => new(this, into: result => { into(result); return ValueTask.CompletedTask; });

    public EventStream<TEvent> Into(Func<WindowResult<TEvent>, ValueTask> into)
        => new(this, into: into);

    public EventStream<TEvent> Into(Func<WindowResult<TEvent>, Task> into)
        => new(this, into: result => new ValueTask(into(result)));

    /// <summary>
    /// Итерирует глобальный поток стора от текущей позиции и доставляет окна в <c>Into</c>.
    /// Возвращает позицию, на которой остановился (для чекпоинта).
    /// </summary>
    public async Task<long> RunAsync(CancellationToken ct = default)
    {
        var buffer = new Dictionary<long, (DateTimeOffset LastTimestamp, List<(DateTimeOffset Timestamp, TEvent Event)> Events)>();

        await foreach (var stored in _store.ReadAllAsync(_fromSequence, batchSize: 1000, ct: ct))
        {
            if (stored.EventType == typeof(TEvent).Name ||
                _serializer.ResolveType(stored.EventType) == typeof(TEvent))
            {
                var @event = _serializer.Deserialize(stored.Data, stored.EventType) as TEvent;
                if (@event is null)
                    continue;

                var index = _window.WindowIndex(stored.Timestamp);
                if (!buffer.TryGetValue(index, out var slot))
                {
                    slot = (stored.Timestamp, new List<(DateTimeOffset, TEvent)>());
                    buffer[index] = slot;
                }
                slot.Events.Add((stored.Timestamp, @event));
            }

            _fromSequence = stored.GlobalSequence;

            // Закрываем окна, которые точно не получат больше событий (старее текущего окна).
            foreach (var key in buffer.Keys.Where(k => k < _window.WindowIndex(DateTimeOffset.UtcNow)).ToArray())
            {
                await EmitAsync(buffer[key], ct).ConfigureAwait(false);
                buffer.Remove(key);
            }
        }

        // End-of-stream flush: оставшиеся открытые окна эмитим по завершении чтения,
        // иначе последний батч событий никогда не уйдёт потребителю.
        foreach (var key in buffer.Keys.OrderBy(k => k).ToArray())
        {
            await EmitAsync(buffer[key], ct).ConfigureAwait(false);
            buffer.Remove(key);
        }

        return _fromSequence;
    }

    private async Task EmitAsync(
        (DateTimeOffset LastTimestamp, List<(DateTimeOffset Timestamp, TEvent Event)> Events) slot,
        CancellationToken ct)
    {
        var events = slot.Events;
        if (events.Count == 0)
            return;

        var groups = new Dictionary<string, List<(DateTimeOffset, TEvent)>>();
        foreach (var item in events)
            (groups.TryGetValue(_keySelector(item.Event), out var list)
                ? list
                : groups[_keySelector(item.Event)] = new List<(DateTimeOffset, TEvent)>())
                .Add(item);

        var results = new Dictionary<string, GroupResult>();
        foreach (var (key, list) in groups)
        {
            var sum = _sum(list.Select(x => x.Item2).ToArray());
            var count = list.Count;
            var average = sum is null ? null : (double?)(sum.Value / count);
            results[key] = new GroupResult(count, sum, average);
        }

        var opened = events.Min(x => x.Timestamp);
        var closed = events.Max(x => x.Timestamp);

        var result = new WindowResult<TEvent>(
            opened.ToUnixTimeMilliseconds(),
            closed.ToUnixTimeMilliseconds(),
            opened,
            closed,
            results);

        await _into(result).ConfigureAwait(false);
    }
}

/// <summary>Entry point: <c>store.Stream&lt;T&gt;()</c>.</summary>
public static class EventStoreStreamExtensions
{
    /// <summary>
    /// Глобальный поток событий типа T поверх Event Store (идея 289):
    /// <c>store.Stream&lt;OrderPaid&gt;().Window(Tumbling(1h)).GroupBy(e=&gt;e.Region).Aggregate(Sum(...)).Into(p =&gt; ...)</c>
    /// </summary>
    public static EventStream<TEvent> Stream<TEvent>(
        this IEventStore store,
        IEventSerializer serializer,
        long fromSequence = 0)
        where TEvent : class
        => new(store, serializer, fromSequence);

    /// <summary>Номер позиции глобального потока для чекпоинта.</summary>
    public static EventStream<TEvent> FromSequence<TEvent>(
        this EventStream<TEvent> stream,
        long fromSequence)
        where TEvent : class
        => stream;
}
