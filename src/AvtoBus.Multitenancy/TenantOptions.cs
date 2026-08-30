namespace AvtoBus.Multitenancy;

/// <summary>Уровень изоляции тенантов (идея 462).</summary>
public enum TenantIsolation
{
    /// <summary>Уровень A: общие очереди + фильтр по тенанту на стороне консьюмера.</summary>
    Shared,

    /// <summary>Уровень B: отдельная очередь per-tenant (адрес destination получает суффикс тенанта).</summary>
    QueuePerTenant,

    /// <summary>Уровень C: виртуальный хост/namespace per-tenant (зависит от транспорта).</summary>
    NamespacePerTenant,
}

/// <summary>
/// Настройки мультитенантности (идеи 461–467): уровень изоляции, реестр тенантов
/// (регион, квоты) и текущий регион сервиса для data-residency проверок.
/// </summary>
public sealed class TenantOptions
{
    /// <summary>Уровень изоляции по умолчанию для тенантов без явного указания.</summary>
    public TenantIsolation Isolation { get; set; } = TenantIsolation.Shared;

    /// <summary>Текущий регион сервиса, например <c>"eu"</c> — источник истины для data-residency.</summary>
    public string? CurrentRegion { get; set; }

    /// <summary>Разрешить ли публикацию в другой регион вообще (глобально). false — жёсткий режим (идея 467).</summary>
    public bool AllowCrossRegion { get; set; }

    /// <summary>Реестр известных тенантов. Пустой — «открытый» режим, тенант не проверяется.</summary>
    public Dictionary<string, TenantRegistration> Tenants { get; } = new(StringComparer.Ordinal);

    /// <summary>Добавляет тенанта. Регион и квоты подхватываются по имени.</summary>
    public TenantOptions AddTenant(string tenantId, Action<TenantRegistration>? configure = null)
    {
        if (Tenants.ContainsKey(tenantId))
            throw new InvalidOperationException($"Tenant '{tenantId}' already registered");
        var registration = new TenantRegistration { TenantId = tenantId };
        configure?.Invoke(registration);
        Tenants[tenantId] = registration;
        return this;
    }
}

/// <summary>Описание одного тенанта: регион размещения данных и квоты (идеи 464, 466, 467).</summary>
public sealed class TenantRegistration
{
    public required string TenantId { get; init; }

    /// <summary>Регион размещения данных тенанта, например <c>"eu"</c>. null — регион не ограничен.</summary>
    public string? Region { get; set; }

    /// <summary>Уровень изоляции именно этого тенанта; null — использовать <see cref="TenantOptions.Isolation"/>.</summary>
    public TenantIsolation? Isolation { get; set; }

    /// <summary>Квоты тенанта: макс. входящих сообщений в секунду. 0 — безлимит (идея 464).</summary>
    public int InboundRatePerSecond { get; set; }

    /// <summary>Разрешить этому тенанту публикации вне его региона (идея 467). По умолчанию — как в options.</summary>
    public bool? AllowCrossRegion { get; set; }
}

/// <summary>
/// Реестр тенантов: конфигурация регионов и квот, собранная в рантайме.
/// Нет состояния — потокобезопасен.
/// </summary>
public sealed class TenantRegistry
{
    private readonly TenantOptions _options;

    public TenantRegistry(TenantOptions options) => _options = options;

    /// <summary>Регион данных тенанта: явный или унаследованный (null — не ограничен).</summary>
    public string? RegionOf(string tenantId)
        => _options.Tenants.TryGetValue(tenantId, out var tenant) ? tenant.Region : null;

    /// <summary>Уровень изоляции тенанта: индивидуальный или общий.</summary>
    public TenantIsolation IsolationOf(string tenantId)
        => _options.Tenants.TryGetValue(tenantId, out var tenant) && tenant.Isolation is { } level
            ? level
            : _options.Isolation;

    /// <summary>Квота входящего трафика тенанта (0 — безлимит).</summary>
    public int InboundRateOf(string tenantId)
        => _options.Tenants.TryGetValue(tenantId, out var tenant) ? tenant.InboundRatePerSecond : 0;

    /// <summary>Все зарегистрированные тенанты — для расширения подписок на их очереди.</summary>
    public IReadOnlyCollection<string> TenantIds => _options.Tenants.Keys;

    /// <summary>Разрешена ли этому тенанту публикация вне его региона.</summary>
    public bool AllowsCrossRegion(string tenantId)
        => _options.Tenants.TryGetValue(tenantId, out var tenant)
           && tenant.AllowCrossRegion is { } allowed
             ? allowed
             : _options.AllowCrossRegion;

    public bool Contains(string tenantId) => _options.Tenants.ContainsKey(tenantId);
}
