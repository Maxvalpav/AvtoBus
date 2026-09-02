using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Actors;

/// <summary>
/// Dapr Virtual Actors + Reminders порт (Go/.NET): actor per entity с single-threaded mailbox, timer/reminder.
/// Активируется по `ActorId`, деактивируется после idle, состояние в `IActorStore`. Напоминания — durable таймеры.
/// Аналог: Dapr `Actor`, Orleans `Grain`, Akka `ShardedActor`.
/// </summary>
public sealed class ActorId(string id)
{
    public string Id => id;
    public override string ToString() => id;
}

public interface IActor
{
    ActorId Id { get; }
}

public interface IActorStore<TState> where TState : class, new()
{
    ValueTask<TState?> LoadAsync(ActorId id, CancellationToken ct);
    ValueTask SaveAsync(ActorId id, TState state, CancellationToken ct);
}

public sealed class InMemoryActorStore<TState> : IActorStore<TState> where TState : class, new()
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, TState> _map = new();
    public ValueTask<TState?> LoadAsync(ActorId id, CancellationToken ct) => ValueTask.FromResult(_map.TryGetValue(id.Id, out var s) ? s : null);
    public ValueTask SaveAsync(ActorId id, TState state, CancellationToken ct) { _map[id.Id] = state; return ValueTask.CompletedTask; }
}

public abstract class VirtualActor<TState> : IActor where TState : class, new()
{
    public required ActorId Id { get; init; }
    protected TState State { get; set; } = new();
    private readonly List<(string name, TimeSpan due, TimeSpan period)> _reminders = [];
    private readonly SemaphoreSlim _mailbox = new(1, 1);

    private readonly Dictionary<string, CancellationTokenSource> _reminderCts = new();
    protected void RegisterReminder(string name, TimeSpan dueTime, TimeSpan period)
    {
        UnregisterReminder(name);
        var cts = new CancellationTokenSource();
        _reminderCts[name] = cts;
        _reminders.RemoveAll(r => r.name == name);
        _reminders.Add((name, dueTime, period));
        _ = RunReminderAsync(name, dueTime, period, cts.Token);
    }
    protected void UnregisterReminder(string name)
    {
        if (_reminderCts.Remove(name, out var cts)) { cts.Cancel(); cts.Dispose(); }
        _reminders.RemoveAll(r => r.name == name);
    }
    private async Task RunReminderAsync(string name, TimeSpan due, TimeSpan period, CancellationToken ct)
    {
        try { await Task.Delay(due, ct).ConfigureAwait(false);
              while (!ct.IsCancellationRequested) { await OnReminderAsync(name, ct).ConfigureAwait(false); await Task.Delay(period, ct).ConfigureAwait(false); } }
        catch (OperationCanceledException) { }
    }

    public async Task ReceiveAsync(object message, CancellationToken ct)
    {
        await _mailbox.WaitAsync(ct).ConfigureAwait(false);
        try { await ReceiveCoreAsync(message, ct).ConfigureAwait(false); }
        finally { _mailbox.Release(); }
    }
    protected abstract Task ReceiveCoreAsync(object message, CancellationToken ct);
    public virtual Task OnActivateAsync(CancellationToken ct) => Task.CompletedTask;
    public virtual Task OnDeactivateAsync(CancellationToken ct) => Task.CompletedTask;
    public virtual Task OnReminderAsync(string name, CancellationToken ct) => Task.CompletedTask;
}

public static class ActorExtensions
{
    public static BusConfigurator AddActors(this BusConfigurator bus)
    {
        bus.Services.AddSingleton(typeof(InMemoryActorStore<>));
        return bus;
    }
}
