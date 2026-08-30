namespace AvtoBus.Multitenancy;

/// <summary>
/// Регион данных сообщения (идея 467). Контракт, помеченный <c>[Region("eu")]</c>, содержит
/// персональные данные этого региона; <see cref="RegionRouteGuard"/> не позволит ему уйти
/// в транспорт другого региона (комплаенс by construction).
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = true)]
public sealed class RegionAttribute(string region) : Attribute
{
    public string Region { get; } = region;
}

/// <summary>
/// Контракт участвует в cross-region репликации (идея 473): outbox-стрим этого типа
/// реплицируется в standby-регион для active-passive failover.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = true)]
public sealed class GeoReplicatedAttribute : Attribute;
