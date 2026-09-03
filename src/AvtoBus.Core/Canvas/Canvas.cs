using AvtoBus.Runtime;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Canvas;

/// <summary>
/// Canvas: компоновка сообщений как конвейер: chain — последовательно, group — параллельно, chord — group + callback.
/// </summary>
public sealed class CanvasChain
{
    private readonly List<(object message, Type type)> _steps = [];

    public CanvasChain Add<T>(T message) where T : class
    {
        _steps.Add((message, typeof(T)));
        return this;
    }

    public IReadOnlyList<(object message, Type type)> Steps => _steps;

    /// <summary>Диспатчит цепочку: отправляет первое сообщение с заголовком `avtobus.chain` содержащим остальные.
    /// Следующий шаг автоматически отправится CanvasMiddleware после успеха хендлера.</summary>
    public async ValueTask DispatchAsync(IBus bus, CancellationToken ct = default)
    {
        if (_steps.Count == 0) return;
        var first = _steps[0];
        var remaining = _steps.Skip(1).Select(s =>
        {
            var json = System.Text.Json.JsonSerializer.Serialize(s.message, s.type);
            return new CanvasStep(s.type.AssemblyQualifiedName!, json);
        }).ToList();
        var opts = new SendOptions();
        if (remaining.Count > 0)
            opts.WithHeader("avtobus.canvas.chain", System.Text.Json.JsonSerializer.Serialize(remaining));
        opts.WithHeader("avtobus.canvas.chain-id", Guid.NewGuid().ToString());
        await DispatchByType(bus, first.message, first.type, opts, ct);
    }

    private static ValueTask DispatchByType(IBus bus, object msg, Type t, SendOptions opts, CancellationToken ct)
    {
        var method = typeof(IBus).GetMethod(nameof(IBus.SendAsync))!.MakeGenericMethod(t);
        return (ValueTask)method.Invoke(bus, [msg, opts, ct])!;
    }
}

public sealed record CanvasStep(string TypeName, object Payload);

public sealed class CanvasGroup
{
    private readonly List<(object message, Type type)> _members = [];
    public CanvasGroup Add<T>(T message) where T : class { _members.Add((message, typeof(T))); return this; }
    public IReadOnlyList<(object message, Type type)> Members => _members;

    public async ValueTask DispatchAsync(IBus bus, CancellationToken ct = default)
    {
        var groupId = Guid.NewGuid().ToString();
        foreach (var (msg, type) in _members)
        {
            var opts = new SendOptions();
            opts.WithHeader("avtobus.canvas.group", groupId);
            opts.WithHeader("avtobus.canvas.group-size", _members.Count.ToString());
            var method = typeof(IBus).GetMethod(nameof(IBus.SendAsync))!.MakeGenericMethod(type);
            await ((ValueTask)method.Invoke(bus, [msg, opts, ct])!).ConfigureAwait(false);
        }
    }
}

/// <summary>
/// Chord: group + callback когда все задачи группы завершены.
/// Реализация через counter в IChordStore (in-memory).
/// </summary>
public sealed class CanvasChord
{
    private readonly CanvasGroup _group;
    private readonly object _callback;
    private readonly Type _callbackType;

    public CanvasChord(CanvasGroup group, object callback, Type callbackType)
    {
        _group = group; _callback = callback; _callbackType = callbackType;
    }

    public async ValueTask DispatchAsync(IBus bus, IChordStore? store = null, CancellationToken ct = default)
    {
        var chordId = Guid.NewGuid().ToString();
        if (store is not null) await store.InitAsync(chordId, _group.Members.Count, _callback, _callbackType, ct);
        foreach (var (msg, type) in _group.Members)
        {
            var opts = new SendOptions();
            opts.WithHeader("avtobus.canvas.chord", chordId);
            opts.WithHeader("avtobus.canvas.group-size", _group.Members.Count.ToString());
            var method = typeof(IBus).GetMethod(nameof(IBus.SendAsync))!.MakeGenericMethod(type);
            await ((ValueTask)method.Invoke(bus, [msg, opts, ct])!).ConfigureAwait(false);
        }
    }
}

public interface IChordStore
{
    ValueTask InitAsync(string chordId, int size, object callback, Type callbackType, CancellationToken ct);
    ValueTask<bool> CompleteOneAsync(string chordId, CancellationToken ct);
    ValueTask<(object callback, Type type)?> TryGetCallbackAsync(string chordId, CancellationToken ct);
}

public sealed class InMemoryChordStore : IChordStore
{
    private sealed record Entry(int Remaining, object Callback, Type Type);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, Entry> _map = new();

    public ValueTask InitAsync(string chordId, int size, object callback, Type type, CancellationToken ct)
    { _map[chordId] = new Entry(size, callback, type); return ValueTask.CompletedTask; }

    public ValueTask<bool> CompleteOneAsync(string chordId, CancellationToken ct)
    {
        while (true)
        {
            if (!_map.TryGetValue(chordId, out var e)) return ValueTask.FromResult(false);
            var next = e with { Remaining = e.Remaining - 1 };
            if (_map.TryUpdate(chordId, next, e))
            {
                if (next.Remaining <= 0)
                    _map.TryRemove(chordId, out _);
                return ValueTask.FromResult(next.Remaining <= 0);
            }
        }
    }

    public ValueTask<(object callback, Type type)?> TryGetCallbackAsync(string chordId, CancellationToken ct)
        => ValueTask.FromResult(_map.TryGetValue(chordId, out var e) && e.Remaining <= 0 ? ((object, Type)?)(e.Callback, e.Type) : null);
}

/// <summary>Фасад компоновки: chain / group / chord.</summary>
public static class Canvas
{
    public static CanvasChain Chain() => new();
    public static CanvasChain Chain<T>(T first) where T : class => new CanvasChain().Add(first);
    public static CanvasGroup Group() => new();
    public static CanvasChord Chord(CanvasGroup group, object callback) => new(group, callback, callback.GetType());
    public static CanvasChord Chord<T>(CanvasGroup group, T callback) where T : class => new(group, callback, typeof(T));
}

/// <summary>
/// Middleware который после успешной обработки проверяет заголовок `avtobus.canvas.chain` и отправляет следующий шаг.
/// </summary>
public sealed class CanvasMiddleware : AvtoBus.Pipeline.IBusMiddleware
{
    private readonly AvtoBusClient _bus;
    private readonly IChordStore? _chordStore;
    public CanvasMiddleware(AvtoBusClient bus, IChordStore? chordStore = null) { _bus = bus; _chordStore = chordStore; }

    public async ValueTask InvokeAsync(ConsumeContext context, AvtoBus.Pipeline.BusDelegate next)
    {
        await next(context);
        if (context.Outcome != ConsumeOutcome.Handled) return;

        // Chain: отправить следующий шаг
        if (context.Envelope.Headers.TryGetValue("avtobus.canvas.chain", out var chainJson) && !string.IsNullOrEmpty(chainJson))
        {
            try
            {
                var remaining = System.Text.Json.JsonSerializer.Deserialize<List<CanvasStep>>(chainJson);
                if (remaining is { Count: > 0 })
                {
                    var nextStep = remaining[0];
                    var nextRemaining = remaining.Skip(1).ToList();
                    var type = Type.GetType(nextStep.TypeName);
                    if (type is not null)
                    {
                        var opts = new SendOptions();
                        if (nextRemaining.Count > 0)
                            opts.WithHeader("avtobus.canvas.chain", System.Text.Json.JsonSerializer.Serialize(nextRemaining));
                        if (context.Envelope.Headers.TryGetValue("avtobus.canvas.chain-id", out var cid))
                            opts.WithHeader("avtobus.canvas.chain-id", cid);
                        // Payload хранится как JSON-строка (см. DispatchAsync), совместимость со старым форматом — JsonElement
                        string payloadJson;
                        if (nextStep.Payload is string s)
                            payloadJson = s;
                        else if (nextStep.Payload is System.Text.Json.JsonElement el)
                            payloadJson = el.GetRawText();
                        else
                            payloadJson = System.Text.Json.JsonSerializer.Serialize(nextStep.Payload);

                        var payload = System.Text.Json.JsonSerializer.Deserialize(payloadJson, type);
                        if (payload is not null)
                        {
                            var send = typeof(IBus).GetMethod(nameof(IBus.SendAsync))!.MakeGenericMethod(type);
                            await ((ValueTask)send.Invoke(_bus, [payload, opts, CancellationToken.None])!).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Chain обрыв логируем, но не ломаем основную обработку
                System.Diagnostics.Debug.WriteLine($"Canvas chain failed: {ex}");
            }
        }

        // Chord: уведомляем store что один участник группы завершён
        if (_chordStore is not null && context.Envelope.Headers.TryGetValue("avtobus.canvas.chord", out var chordId) && chordId is not null)
        {
            var done = await _chordStore.CompleteOneAsync(chordId, context.CancellationToken);
            if (done)
            {
                var cb = await _chordStore.TryGetCallbackAsync(chordId, context.CancellationToken);
                if (cb is not null)
                {
                    var method = typeof(IBus).GetMethod(nameof(IBus.SendAsync))!.MakeGenericMethod(cb.Value.type);
                    await ((ValueTask)method.Invoke(_bus, [cb.Value.callback, null, CancellationToken.None])!).ConfigureAwait(false);
                }
            }
        }
    }
}

public static class CanvasExtensions
{
    public static AvtoBus.Configuration.BusConfigurator UseCanvas(this AvtoBus.Configuration.BusConfigurator bus, IChordStore? store = null)
    {
        store ??= new InMemoryChordStore();
        bus.Services.AddSingleton(store);
        bus.Services.AddSingleton<CanvasMiddleware>();
        bus.Pipeline(b => b.Use<CanvasMiddleware>());
        return bus;
    }
}
