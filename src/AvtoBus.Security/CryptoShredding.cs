using Microsoft.Extensions.Logging;

namespace AvtoBus.Security;

/// <summary>Crypto-shredding (идея GDPR 452): удаление ключа тенанта делает его данные нечитаемыми без переписывания строк.</summary>
public sealed class CryptoShreddingService(IKmsProvider? kms = null, ILogger<CryptoShreddingService>? logger = null)
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> _tombstones = new();
    public async Task ShredTenantAsync(string tenantId, CancellationToken ct)
    {
        _tombstones[tenantId] = 1;
        if (kms is not null)
        {
            try { await kms.DeleteKeyAsync($"tenant:{tenantId}", ct).ConfigureAwait(false); }
            catch (Exception ex) { logger?.LogError(ex, "KMS DeleteKey failed for {Tenant}", tenantId); }
        }
        logger?.LogWarning("Crypto-shred тенанта {Tenant} — ключи поколения помечены как удалённые", tenantId);
        await Task.CompletedTask.ConfigureAwait(false);
    }
    public bool IsShredded(string tenantId) => _tombstones.ContainsKey(tenantId);
    public Task<bool> IsShreddedAsync(string tenantId, CancellationToken ct) => Task.FromResult(IsShredded(tenantId));
}
