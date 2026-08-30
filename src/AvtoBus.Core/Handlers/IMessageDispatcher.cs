namespace AvtoBus.Handlers;

/// <summary>
/// Опциональная возможность диспетчера: сообщить лимит времени обработки из
/// <see cref="HandlerTimeoutAttribute"/> (идея 170). Хостовый middleware применяет его.
/// </summary>
public interface IHandlerTimeoutProvider
{
    TimeSpan? Timeout { get; }
}

/// <summary>
/// Опциональная возможность диспетчера: сообщить требование авторизации из
/// <see cref="BusAuthorizeAttribute"/> (идея 453). Хостовый middleware применяет его,
/// если ни один обработчик сообщения не требует авторизации — requirement равен null.
/// </summary>
public interface IHandlerAuthorizationProvider
{
    BusAuthorizeAttribute? Authorization { get; }
}

/// <summary>
/// Вызывает один конкретный хендлер для одного сообщения.
/// Экземпляры строятся при старте; на горячем пути — только вызов делегата.
/// </summary>
public interface IMessageDispatcher
{
    /// <summary>CLR-тип сообщения, который умеет обрабатывать этот диспетчер.</summary>
    Type MessageType { get; }

    /// <summary>Имя для диагностики и метрик: <c>OrderHandlers.Handle</c>.</summary>
    string HandlerName { get; }

    ValueTask DispatchAsync(ConsumeContext context);
}

/// <summary>Диспетчер поверх готовой лямбды — уровень 3 API.</summary>
internal sealed class DelegateDispatcher(
    Type messageType,
    string handlerName,
    Func<ConsumeContext, ValueTask> handler) : IMessageDispatcher
{
    public Type MessageType { get; } = messageType;

    public string HandlerName { get; } = handlerName;

    public ValueTask DispatchAsync(ConsumeContext context) => handler(context);
}
