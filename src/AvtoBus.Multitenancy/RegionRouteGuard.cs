using AvtoBus.Multitenancy;
using AvtoBus.Configuration;

namespace AvtoBus.Multitenancy;

/// <summary>
/// Data-residency guard (идея 467): сообщение, чей тенант размещён в регионе X, не может быть
/// отправлено сервисом, работающим в регионе Y. Проверка происходит на исходящем пути,
/// до транспорта — комплаенс by construction, а не по надежде.
/// </summary>
public sealed class RegionRouteGuard(TenantRegistry registry, TenantOptions options) : IRegionPolicy
{
    public void Validate(Envelope envelope, TransportDestination destination)
    {
        var tenantId = envelope.TenantId;
        if (tenantId is null)
            return;

        // Тенант без привязки к региону — политике нечего проверять.
        var region = registry.RegionOf(tenantId);
        if (region is null || options.CurrentRegion is null)
            return;

        if (string.Equals(region, options.CurrentRegion, StringComparison.OrdinalIgnoreCase))
            return;

        if (registry.AllowsCrossRegion(tenantId))
            return;

        throw new RegionViolationException(
            $"Сообщение {envelope.MessageType} тенанта {tenantId} (регион '{region}') " +
            $"не может быть отправлено из региона '{options.CurrentRegion}'");
    }
}
