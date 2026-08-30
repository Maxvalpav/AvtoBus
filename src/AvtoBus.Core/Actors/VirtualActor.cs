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

    protected void RegisterReminder(string name, TimeSpan dueTime, TimeSpan period) => _reminders.Add((name, dueTime, period));
    protected void UnregisterReminder(string name) => _reminders.RemoveAll(r => r.name == name);

    public abstract Task ReceiveAsync(object message, CancellationToken ct);
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
