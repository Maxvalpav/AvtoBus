using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Sagas;

/// <summary>Регистрация саг в шине (док 17, §1).</summary>
public static class SagaRegistration
{
    /// <summary>
    /// Регистрирует сагу: метаданные (корреляции + инварианты), хранилище по умолчанию
    /// и по одному диспетчеру на каждое коррелируемое сообщение.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode(
        "Метаданные саги строятся сканированием методов/корреляций через рефлексию — несовместимо с trimming/AOT.")]
    [System.Diagnostics.CodeAnalysis.RequiresDynamicCode(
        "Диспетчеры саги компилируются через Expression — несовместимо с NativeAOT.")]
    public static BusConfigurator AddSaga<
        [System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicConstructors |
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicMethods |
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.NonPublicMethods)] TSaga,
        TState>(this BusConfigurator bus)
        where TSaga : Saga<TState>, new()
        where TState : SagaState, new()
    {
        var meta = SagaMetadata<TSaga, TState>.Build();

        bus.Services.TryAddSingleton<ISagaStore, InMemorySagaStore>();
        bus.Services.TryAddScoped<TSaga>();
        bus.Services.AddSingleton(meta);

        foreach (var correlation in meta.Correlations)
            bus.AddDispatcher(new SagaDispatcher<TSaga, TState>(meta, correlation));

        return bus;
    }
}
