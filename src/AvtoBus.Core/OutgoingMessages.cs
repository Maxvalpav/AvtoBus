using System.Collections;

namespace AvtoBus;

public enum OutgoingKind
{
    Send,
    Publish,
    Respond,
}

/// <param name="Message">Тело исходящего сообщения.</param>
/// <param name="Kind">Команда, событие или ответ.</param>
/// <param name="Options">Переопределения конверта; <c>null</c> — всё по умолчанию.</param>
public readonly record struct OutgoingMessage(object Message, OutgoingKind Kind, MessageOptions? Options);

/// <summary>
/// Динамический билдер каскадов (идея 3). Возвращается из хендлера, когда набор исходящих
/// сообщений зависит от условий.
/// </summary>
public sealed class OutgoingMessages : IEnumerable<OutgoingMessage>
{
    private readonly List<OutgoingMessage> _messages = [];

    public int Count => _messages.Count;

    public OutgoingMessages Send<T>(T command, SendOptions? options = null) where T : class
    {
        _messages.Add(new OutgoingMessage(command, OutgoingKind.Send, options));
        return this;
    }

    public OutgoingMessages Publish<T>(T @event, PublishOptions? options = null) where T : class
    {
        _messages.Add(new OutgoingMessage(@event, OutgoingKind.Publish, options));
        return this;
    }

    public OutgoingMessages Schedule<T>(T message, TimeSpan delay, TimeProvider? timeProvider = null) where T : class
    {
        var now = (timeProvider ?? TimeProvider.System).GetUtcNow();
        _messages.Add(new OutgoingMessage(
            message,
            OutgoingKind.Send,
            new SendOptions { DeliverAt = now + delay }));
        return this;
    }

    public OutgoingMessages Schedule<T>(T message, DateTimeOffset at) where T : class
    {
        _messages.Add(new OutgoingMessage(message, OutgoingKind.Send, new SendOptions { DeliverAt = at }));
        return this;
    }

    public OutgoingMessages RespondTo<T>(T reply) where T : class
    {
        _messages.Add(new OutgoingMessage(reply, OutgoingKind.Respond, null));
        return this;
    }

    public IEnumerator<OutgoingMessage> GetEnumerator() => _messages.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
