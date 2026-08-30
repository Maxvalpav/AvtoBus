using AvtoBus;

namespace AvtoBus.Multitenancy;

/// <summary>
/// Изоляция тенантов на уровне хранилища (идея 462, уровни B/C):
///
/// — <see cref="TenantIsolation.QueuePerTenant"/> (B): имя destination получает суффикс
///   тенанта (<c>orders.acme</c>) — у каждого тенанта своя физическая очередь;
/// — <see cref="TenantIsolation.NamespacePerTenant"/> (C): имя destination получает префикс
///   тенанта (<c>acme.orders</c>) — моделирует отдельный namespace/vhost.
///
/// <see cref="TenantIsolation.Shared"/> (A) — назначение не меняется: все тенанты живут
/// в общей очереди, изоляция остаётся на уровне консьюмера.
/// </summary>
public sealed class TenantIsolationPolicy(TenantRegistry registry) : ITenantIsolationPolicy
{
    public IReadOnlyCollection<string> TenantIds => registry.TenantIds;

    public TransportDestination Isolate(TransportDestination destination, string tenantId)
    {
        var safe = SanitizeTenantId(tenantId);
        return registry.IsolationOf(tenantId) switch
        {
            TenantIsolation.QueuePerTenant =>
                destination with { Name = $"{destination.Name}.{safe}" },

            TenantIsolation.NamespacePerTenant =>
                destination with { Name = $"{safe}.{destination.Name}" },

            _ => destination,
        };
    }

    private static string SanitizeTenantId(string tenantId)
    {
        var sb = new System.Text.StringBuilder(tenantId.Length);
        foreach (var ch in tenantId)
            sb.Append(char.IsLetterOrDigit(ch) || ch == '-' || ch == '_' ? ch : '_');
        return sb.Length == 0 ? "unknown" : sb.ToString();
    }
}
