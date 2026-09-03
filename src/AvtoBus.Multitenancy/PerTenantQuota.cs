namespace AvtoBus.Multitenancy;

/// <summary>
/// Per-tenant quota: лимит сообщений в секунду на tenant, сверх — в DLQ.
/// </summary>
public sealed class PerTenantQuota
{
    private readonly Dictionary<string, int> _limits = new();
    public void Set(string tenant, int perSecond) => _limits[tenant] = perSecond;
    public int Get(string tenant) => _limits.TryGetValue(tenant, out var v) ? v : 1000;
    public bool IsOverLimit(string tenant, int currentPerSecond) => currentPerSecond > Get(tenant);
}
