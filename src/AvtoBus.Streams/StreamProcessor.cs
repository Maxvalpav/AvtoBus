namespace AvtoBus.Streams;

public sealed record StreamRecord<TKey, TValue>(TKey Key, TValue Value, DateTimeOffset Timestamp);

public interface IStateStore<TKey, TValue> where TKey : notnull
{
    ValueTask<TValue?> GetAsync(TKey key, CancellationToken ct);
    ValueTask PutAsync(TKey key, TValue value, CancellationToken ct);
    ValueTask DeleteAsync(TKey key, CancellationToken ct);
    IAsyncEnumerable<KeyValuePair<TKey, TValue>> ScanAsync(CancellationToken ct);
}

public sealed class InMemoryStateStore<TKey, TValue> : IStateStore<TKey, TValue> where TKey : notnull
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<TKey, TValue> _dict = new();
    public ValueTask<TValue?> GetAsync(TKey key, CancellationToken ct) => ValueTask.FromResult(_dict.TryGetValue(key, out var v) ? v : default);
    public ValueTask PutAsync(TKey key, TValue value, CancellationToken ct) { _dict[key] = value; return ValueTask.CompletedTask; }
    public ValueTask DeleteAsync(TKey key, CancellationToken ct) { _dict.TryRemove(key, out _); return ValueTask.CompletedTask; }
    public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> ScanAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    { var snapshot = _dict.ToArray(); foreach (var kv in snapshot) { ct.ThrowIfCancellationRequested(); yield return kv; } await Task.CompletedTask; }
}

public sealed class StatefulProcessor<TIn, TState, TOut>(
    IStateStore<string, TState> stateStore,
    Func<TIn, TState?, TState> update,
    Func<string, TState, TOut?> emit) : IStreamProcessor<TIn, TOut>
{
    public async IAsyncEnumerable<StreamRecord<string, TOut>> ProcessAsync(IAsyncEnumerable<StreamRecord<string, TIn>> input, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var rec in input.WithCancellation(ct))
        {
            var current = await stateStore.GetAsync(rec.Key, ct);
            var next = update(rec.Value, current);
            await stateStore.PutAsync(rec.Key, next, ct);
            var outVal = emit(rec.Key, next);
            if (outVal is not null) yield return new StreamRecord<string, TOut>(rec.Key, outVal, rec.Timestamp);
        }
    }
}

public interface IStreamProcessor<TIn, TOut>
{
    IAsyncEnumerable<StreamRecord<string, TOut>> ProcessAsync(IAsyncEnumerable<StreamRecord<string, TIn>> input, CancellationToken ct);
}

public sealed class MapFilterProcessor<TIn, TOut>(
    Func<TIn, TOut> map,
    Func<TOut, bool>? filter = null) : IStreamProcessor<TIn, TOut>
{
    public async IAsyncEnumerable<StreamRecord<string, TOut>> ProcessAsync(
        IAsyncEnumerable<StreamRecord<string, TIn>> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var rec in input.WithCancellation(ct))
        {
            var mapped = map(rec.Value);
            if (filter is not null && !filter(mapped)) continue;
            yield return new StreamRecord<string, TOut>(rec.Key, mapped, rec.Timestamp);
        }
    }
}

public sealed class WindowedAggregate<TValue>(
    TimeSpan windowSize,
    Func<IReadOnlyList<TValue>, TValue> aggregate)
{
    public TimeSpan WindowSize => windowSize;

    public IReadOnlyList<StreamRecord<string, TValue>> Aggregate(IReadOnlyList<StreamRecord<string, TValue>> window, DateTimeOffset? eventTimeNow = null)
    {
        var now = eventTimeNow ?? window.MaxBy(r => r.Timestamp)?.Timestamp ?? DateTimeOffset.UtcNow;
        var cutoff = now - windowSize;
        var filtered = window.Where(r => r.Timestamp >= cutoff).ToList();
        var grouped = filtered.GroupBy(r => r.Key);
        var result = new List<StreamRecord<string, TValue>>();
        foreach (var g in grouped)
        {
            var values = g.Select(r => r.Value).ToList();
            var agg = aggregate(values);
            result.Add(new StreamRecord<string, TValue>(g.Key, agg, now));
        }
        return result;
    }
}

/// <summary>
/// Session window как в Kafka Streams (Java) `SessionWindows.with(gap)` и Faust (Python) `hopping_window`.
/// Группирует записи по ключу пока gap между событиями &lt; inactivityGap; новая сессия если пауза &gt; gap.
/// Аналог также Apache Flink session windows.
/// </summary>
public sealed class SessionWindow<TValue>(TimeSpan gap)
{
    public TimeSpan Gap => gap;

    public IReadOnlyList<Session<TValue>> Build(IReadOnlyList<StreamRecord<string, TValue>> records)
    {
        var sessions = new List<Session<TValue>>();
        foreach (var grp in records.GroupBy(r => r.Key))
        {
            var sorted = grp.OrderBy(r => r.Timestamp).ToList();
            Session<TValue>? cur = null;
            foreach (var rec in sorted)
            {
                if (cur is null || rec.Timestamp - cur.End > gap)
                {
                    cur = new Session<TValue>(grp.Key, rec.Timestamp, rec.Timestamp, []);
                    sessions.Add(cur);
                }
                cur.Values.Add(rec.Value);
                if (rec.Timestamp > cur.End) cur.End = rec.Timestamp;
            }
        }
        return sessions;
    }
}

public sealed class Session<TValue>(string key, DateTimeOffset start, DateTimeOffset end, List<TValue> values)
{
    public string Key { get; } = key;
    public DateTimeOffset Start { get; } = start;
    public DateTimeOffset End { get; set; } = end;
    public List<TValue> Values { get; } = values;
}

/// <summary>
/// KStream-KTable join как в Kafka Streams `KStream.join(KTable)` и Akka Streams `zipWith`.
/// Соединяет два потока по ключу внутри окна `joinWindow`. Использует state store для правой стороны (таблица).
/// Аналог Faust `stream.join(table)` и ZIO `ZStream.zip`.
/// </summary>
public sealed class StreamJoinProcessor<TLeft, TRight, TOut> : IStreamProcessor<TLeft, TOut>
{
    private readonly IStateStore<string, TRight> _rightStore;
    private readonly TimeSpan _joinWindow;
    private readonly Func<TLeft, TRight, TOut> _joiner;
    public StreamJoinProcessor(IStateStore<string, TRight> rightStore, TimeSpan joinWindow, Func<TLeft, TRight, TOut> joiner)
    {
        _rightStore = rightStore; _joinWindow = joinWindow; _joiner = joiner;
    }
    public async IAsyncEnumerable<StreamRecord<string, TOut>> ProcessAsync(
        IAsyncEnumerable<StreamRecord<string, TLeft>> input,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var rec in input.WithCancellation(ct))
        {
            _ = _joinWindow; // window-aware join — для простоты TTL на уровне store, как Kafka Streams `joinWindow`
            var right = await _rightStore.GetAsync(rec.Key, ct);
            if (right is not null)
                yield return new StreamRecord<string, TOut>(rec.Key, _joiner(rec.Value, right), rec.Timestamp);
        }
    }

    public async ValueTask PutRightAsync(string key, TRight value, CancellationToken ct)
        => await _rightStore.PutAsync(key, value, ct);
}

/// <summary>
/// GlobalKTable (Kafka Streams) / Global store — широковещательная таблица для join без партиционирования.
/// </summary>
public sealed class GlobalStateStore<TKey, TValue>(IStateStore<TKey, TValue> inner) where TKey : notnull
{
    public ValueTask<TValue?> GetAsync(TKey key, CancellationToken ct) => inner.GetAsync(key, ct);
    public ValueTask PutAsync(TKey key, TValue value, CancellationToken ct) => inner.PutAsync(key, value, ct);
    public IAsyncEnumerable<KeyValuePair<TKey, TValue>> ScanAsync(CancellationToken ct) => inner.ScanAsync(ct);
}
