using AvtoBus.Handlers;

namespace AvtoBus.Dispatching;

/// <summary>
/// Мост между Source Generator-ом и рантаймом: сгенерированные диспетчеры регистрируются
/// здесь через <c>[ModuleInitializer]</c> при загрузке сборки (док 16, §4).
/// <see cref="BusConfigurator"/> вычитывает их на этапе конфигурации и подмешивает в
/// <see cref="DispatcherRegistry"/>, избегая дублей с reflection-обнаружением.
/// </summary>
public static class AvtoBusRegistry
{
    private static readonly object Sync = new();
    private static readonly List<(Type HandlerType, IMessageDispatcher Dispatcher)> Registered = [];

    /// <summary>
    /// Регистрирует сгенерированный диспетчер. Вызывается только из сгенерированного кода
    /// с <c>typeof()</c> — trim-safe по построению, аннотация фиксирует это для анализатора.
    /// </summary>
    public static void Register(
        [System.Diagnostics.CodeAnalysis.DynamicallyAccessedMembers(
            System.Diagnostics.CodeAnalysis.DynamicallyAccessedMemberTypes.PublicConstructors)] Type handlerType,
        IMessageDispatcher dispatcher)
    {
        lock (Sync)
            Registered.Add((handlerType, dispatcher));
    }

    /// <summary>Все зарегистрированные диспетчеры.</summary>
    public static IReadOnlyList<(Type HandlerType, IMessageDispatcher Dispatcher)> Dispatchers
    {
        get
        {
            lock (Sync)
                return Registered.ToArray();
        }
    }

    /// <summary>
    /// Есть ли сгенерированные диспетчеры для конкретного типа-хендлера.
    /// Если да — reflection-обнаружение для этого типа не нужно (идея 401).
    /// </summary>
    public static bool HasGeneratedFor(Type handlerType)
    {
        lock (Sync)
            return Registered.Any(entry => entry.HandlerType == handlerType);
    }

    /// <summary>Сгенерированные диспетчеры для типа-хендлера.</summary>
    public static IReadOnlyList<IMessageDispatcher> ForHandlerType(Type handlerType)
    {
        lock (Sync)
            return Registered.Where(entry => entry.HandlerType == handlerType).Select(e => e.Dispatcher).ToArray();
    }

    /// <summary>Все типы-хендлеры, покрытые генератором (AOT-safe).</summary>
    public static IReadOnlyList<Type> GeneratedHandlerTypes
    {
        get
        {
            lock (Sync)
                return Registered.Select(e => e.HandlerType).Distinct().ToArray();
        }
    }
}
