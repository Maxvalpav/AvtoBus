namespace AvtoBus;

/// <summary>
/// Команда — ровно один получатель. Отправляется через <see cref="IBus.Send"/> (идея 10).
/// </summary>
public interface ICommand;

/// <summary>
/// Событие — 0..N получателей. Публикуется через <see cref="IBus.Publish"/> (идея 10).
/// </summary>
public interface IEvent;

/// <summary>Запрос с типизированным ответом.</summary>
public interface IRequest<TReply>;

/// <summary>
/// Стабильное имя контракта на проводе. Переименование C#-класса не ломает совместимость (идея 102).
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Interface)]
public sealed class MessageAliasAttribute(string name, params string[] legacyNames) : Attribute
{
    public string Name { get; } = name;

    /// <summary>Старые имена, которые тоже принимаются на десериализации (идея 103).</summary>
    public IReadOnlyList<string> LegacyNames { get; } = legacyNames;
}

/// <summary>Переопределяет очередь/топик, вычисляемые по конвенции.</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class TopicAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}

/// <summary>Поле, из которого берётся ключ партиционирования (идея 58).</summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
public sealed class PartitionKeyAttribute : Attribute;

/// <summary>Транспортные хинты контракта (идея 93).</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class MessageAttribute : Attribute
{
    public bool Durable { get; init; } = true;
    public int Priority { get; init; }

    /// <summary>TTL в формате <see cref="TimeSpan"/>, например <c>"00:05:00"</c>.</summary>
    public string? Ttl { get; init; }
}

/// <summary>Ограничивает время работы хендлера; по истечении взводится CancellationToken (идея 170).</summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
public sealed class HandlerTimeoutAttribute(string timeout) : Attribute
{
    public TimeSpan Timeout { get; } = TimeSpan.TryParse(
        timeout, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
        ? parsed
        : throw new ArgumentException($"Некорректный таймаут '{timeout}'. Формат — инвариантный TimeSpan, например \"00:05:00\".", nameof(timeout));
}

/// <summary>
/// Требование авторизации для хендлера (идея 453): роли и/или логическая политика.
/// ClaimsPrincipal восстанавливается из подписанного пользовательского контекста конверта (идея 454)
/// и проверяется <see cref="AvtoBus.Pipeline.IAuthorizer"/> перед вызовом хендлера.
/// </summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class, AllowMultiple = false)]
public sealed class BusAuthorizeAttribute : Attribute
{
    /// <summary>Пользователь обязан иметь все перечисленные роли (OR с <see cref="Policy"/>).</summary>
    public string[] Roles { get; init; } = [];

    /// <summary>Имя назначенной политики. null — только роли.</summary>
    public string? Policy { get; init; }

    /// <summary>Требуется ли аутентифицированный пользователь («anonymous» запрещён).</summary>
    public bool RequireAuthenticated { get; init; } = true;
}

/// <summary>Событие, потерю которого можно допустить при перегрузке (идея 96).</summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class LossyAttribute : Attribute;

/// <summary>Семантика события (идея 144).</summary>
public enum MessageSemantics
{
    /// <summary>Свершившийся факт.</summary>
    Fact,

    /// <summary>Изменение относительно предыдущего состояния.</summary>
    Delta,

    /// <summary>Полное состояние на момент времени.</summary>
    Snapshot,
}

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class SemanticsAttribute(MessageSemantics semantics) : Attribute
{
    public MessageSemantics Semantics { get; } = semantics;
}
