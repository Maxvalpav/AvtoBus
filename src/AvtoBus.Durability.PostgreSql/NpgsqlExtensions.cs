using AvtoBus.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Durability.PostgreSql;

/// <summary>
/// PostgreSQL durability — обёртка над Outbox.EfCore для Npgsql (feat 1 по порядку — Workflow).
/// Заменён AddAvtoBusEfCoreStores на AddAvtoBus + UseOutbox из 8-Power-Clean.
/// </summary>
public static class NpgsqlExtensions
{
    public static IServiceCollection AddAvtoBusNpgsql<
        [System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicConstructors |
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.NonPublicConstructors |
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicProperties)] TDbContext>(
        this IServiceCollection services, string connectionString)
        where TDbContext : DbContext
    {
        services.AddDbContext<TDbContext>(o => o.UseNpgsql(connectionString, npg => npg.EnableRetryOnFailure()));
        // Outbox уже через bus.UseOutbox<TDbContext>() в Program.cs
        return services;
    }
}
