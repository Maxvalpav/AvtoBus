namespace AvtoBus;

/// <summary>
/// Уровень 1 API: хендлер как класс. Явно и знакомо тем, кто пришёл с MassTransit/Rebus.
/// </summary>
public interface IConsumer<T> where T : class
{
    Task ConsumeAsync(ConsumeContext<T> context);
}

/// <summary>
/// Хендлер второй линии обороны: получает сообщение, исчерпавшее все ретраи (идея 169).
/// </summary>
public interface IFailedConsumer<T> where T : class
{
    Task ConsumeAsync(IFailed<T> failed, ConsumeContext context);
}

/// <summary>Сообщение, провалившее все попытки обработки.</summary>
public interface IFailed<out T> where T : class
{
    T Message { get; }

    Envelope Envelope { get; }

    string ErrorDescription { get; }

    Exception? Exception { get; }

    int Attempts { get; }
}

internal sealed record FailedMessage<T>(
    T Message,
    Envelope Envelope,
    string ErrorDescription,
    Exception? Exception,
    int Attempts) : IFailed<T> where T : class;

/// <summary>Строит типизированный <c>IFailed&lt;T&gt;</c> без рантайм-дженерик-кода в вызывающем.</summary>
internal static class FailedMessageFactory
{
    public static object Create(Type messageType, object message, Envelope envelope, string description, Exception? exception, int attempts)
    {
        var closed = typeof(FailedMessage<>).MakeGenericType(messageType);
        return System.Activator.CreateInstance(closed, message, envelope, description, exception, attempts)!;
    }
}

/// <summary>
/// Батч-хендлер: одна обработка на N сообщений (идея 19).
/// Позволяет схлопнуть 500 событий в один INSERT.
/// </summary>
public interface IMessageBatch<out T> where T : class
{
    IReadOnlyList<T> Messages { get; }

    IReadOnlyList<ConsumeContext> Contexts { get; }

    int Count { get; }
}

internal sealed class MessageBatch<T>(IReadOnlyList<T> messages, IReadOnlyList<ConsumeContext> contexts)
    : IMessageBatch<T> where T : class
{
    public IReadOnlyList<T> Messages { get; } = messages;

    public IReadOnlyList<ConsumeContext> Contexts { get; } = contexts;

    public int Count => Messages.Count;
}
