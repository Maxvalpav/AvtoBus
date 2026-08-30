using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.EventSourcing.Projections;

/// <summary>
/// Базовый класс проекции с чекпоинтом в таблице <c>avtobus_projection_checkpoints</c>
/// (пользовательский TDbContext + ConfigureEventSourcing), идея 254.
/// </summary>
public abstract class EfCoreProjection<TDbContext> : Projection
    where TDbContext : DbContext
{
    protected readonly IServiceScopeFactory Scopes;

    protected EfCoreProjection(IServiceScopeFactory scopes) => Scopes = scopes;

    public override async ValueTask<long> GetCheckpointAsync(CancellationToken ct)
    {
        await using var scope = Scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();
        var cp = await db.Set<EsProjectionCheckpoint>()
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.ProjectionName == Name, ct);
        return cp?.Position ?? 0;
    }

    public override async ValueTask SaveCheckpointAsync(long position, CancellationToken ct)
    {
        await using var scope = Scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();

        var cp = await db.Set<EsProjectionCheckpoint>().FirstOrDefaultAsync(x => x.ProjectionName == Name, ct);
        if (cp is null)
        {
            db.Set<EsProjectionCheckpoint>().Add(new EsProjectionCheckpoint
            {
                ProjectionName = Name,
                Position = position,
                UpdatedAt = DateTimeOffset.UtcNow,
            });
        }
        else
        {
            cp.Position = position;
            cp.UpdatedAt = DateTimeOffset.UtcNow;
            cp.LastError = null;
        }

        await db.SaveChangesAsync(ct);
    }

    public override async ValueTask ResetAsync(CancellationToken ct)
    {
        await using var scope = Scopes.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();

        var cp = await db.Set<EsProjectionCheckpoint>().FirstOrDefaultAsync(x => x.ProjectionName == Name, ct);
        if (cp is not null)
            cp.Position = 0;
        await db.SaveChangesAsync(ct);
    }
}
