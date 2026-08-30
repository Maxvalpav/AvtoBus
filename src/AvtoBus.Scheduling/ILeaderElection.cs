namespace AvtoBus.Scheduling;

/// <summary>
/// Выбор лидера для single-instance задач в кластере (идея 224). Реализации:
/// <see cref="InMemoryLeaderElection"/> для одиночного инстанса и EF Core
/// <see cref="EfCoreLeaderElection{T}"/> — одна запирающая строка на ресурс.
/// </summary>
public interface ILeaderElection
{
    ValueTask<bool> TryAcquireAsync(string resource, TimeSpan lease, CancellationToken ct = default);
    ValueTask<bool> RenewAsync(string resource, TimeSpan lease, CancellationToken ct = default);
    ValueTask ReleaseAsync(string resource, CancellationToken ct = default);
    bool IsLeader(string resource);
}

/// <summary>
/// In-memory leader election: всегда лидер единственного инстанса (эталон для тестов и монолита).
/// </summary>
public sealed class InMemoryLeaderElection : ILeaderElection
{
    private readonly object _gate = new();
    private readonly Dictionary<string, bool> _held = new(StringComparer.Ordinal);

    public ValueTask<bool> TryAcquireAsync(string resource, TimeSpan lease, CancellationToken ct = default)
    {
        lock (_gate)
        {
            if (_held.TryGetValue(resource, out var isLeader))
                return ValueTask.FromResult(isLeader);
            _held[resource] = true;
            return ValueTask.FromResult(true);
        }
    }

    public ValueTask<bool> RenewAsync(string resource, TimeSpan lease, CancellationToken ct = default)
    {
        lock (_gate)
            return ValueTask.FromResult(_held.GetValueOrDefault(resource));
    }

    public ValueTask ReleaseAsync(string resource, CancellationToken ct = default)
    {
        lock (_gate)
            _held.Remove(resource);
        return ValueTask.CompletedTask;
    }

    public bool IsLeader(string resource)
    {
        lock (_gate)
            return _held.GetValueOrDefault(resource);
    }
}
