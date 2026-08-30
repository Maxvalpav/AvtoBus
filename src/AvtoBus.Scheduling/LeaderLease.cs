using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Scheduling;

/// <summary>
/// Лидер-элекшн через одну строку на ресурс в EF Core: пессимистичный атомарный acquire
/// по уникальному имени + проверка lease-таймстампа. Позволяет «одной партии» cron
/// работать на единственной реплике кластера без брокерской поддержки (идея 224).
/// </summary>
public sealed class EfCoreLeaderElection<TDb> : ILeaderElection
    where TDb : DbContext
{
    private readonly IServiceScopeFactory _scopes;
    private readonly TimeProvider _clock;
    private readonly HashSet<string> _held = new(StringComparer.Ordinal);

    public EfCoreLeaderElection(IServiceScopeFactory scopes, TimeProvider clock)
    {
        _scopes = scopes;
        _clock = clock;
    }

    public async ValueTask<bool> TryAcquireAsync(string resource, TimeSpan lease, CancellationToken ct = default)
    {
        var now = _clock.GetUtcNow().UtcDateTime;
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        var row = await db.Set<LeaderLease>()
            .FirstOrDefaultAsync(x => x.Resource == resource, ct);

        while (true)
        {
            if (row is not null && row.ExpiresAt > now)
            {
                // Lease живёт у другого инстанса — лидером не стать.
                return false;
            }

            if (row is null)
            {
                db.Set<LeaderLease>().Add(new LeaderLease
                {
                    Resource = resource,
                    LeaseOwner = Environment.MachineName,
                    ExpiresAt = now.Add(lease),
                });
            }
            else
            {
                row.LeaseOwner = Environment.MachineName;
                row.ExpiresAt = now.Add(lease);
            }

            try
            {
                await db.SaveChangesAsync(ct);
                _held.Add(resource);
                return true;
            }
            catch (DbUpdateException)
            {
                // Другой инстанс успел занять строку (или продлить) в этот момент.
                // Перечитываем актуальное состояние и пробуем снова.
                row = await db.Set<LeaderLease>().AsNoTracking()
                    .FirstOrDefaultAsync(x => x.Resource == resource, ct);
                now = _clock.GetUtcNow().UtcDateTime;
            }
        }
    }

    public async ValueTask<bool> RenewAsync(string resource, TimeSpan lease, CancellationToken ct = default)
    {
        var now = _clock.GetUtcNow().UtcDateTime;
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        var rows = await db.Set<LeaderLease>()
            .Where(x => x.Resource == resource && x.LeaseOwner == Environment.MachineName)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.ExpiresAt, now.Add(lease)), ct);

        return rows > 0;
    }

    public async ValueTask ReleaseAsync(string resource, CancellationToken ct = default)
    {
        await using var scope = _scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDb>();

        await db.Set<LeaderLease>()
            .Where(x => x.Resource == resource && x.LeaseOwner == Environment.MachineName)
            .ExecuteDeleteAsync(ct);

        _held.Remove(resource);
    }

    public bool IsLeader(string resource) => _held.Contains(resource);
}

/// <summary>Запирающая строка лидера: одна на ресурс, специальные данные не хранит.</summary>
public sealed class LeaderLease
{
    public long Id { get; set; }
    public string Resource { get; set; } = "";
    public string LeaseOwner { get; set; } = "";
    public DateTime ExpiresAt { get; set; }
}

public static class LeaderLeaseModelBuilder
{
    public static ModelBuilder ConfigureLeaderLease(this ModelBuilder mb)
    {
        mb.Entity<LeaderLease>(e =>
        {
            e.ToTable("avtobus_leader_lease");
            e.HasKey(x => x.Id);
            e.HasIndex(x => x.Resource).IsUnique();
            e.HasIndex(x => x.ExpiresAt);
        });
        return mb;
    }
}
