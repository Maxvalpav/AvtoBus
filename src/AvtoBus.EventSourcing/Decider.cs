namespace AvtoBus.EventSourcing;

/// <summary>
/// Функциональный стиль (Decider pattern): чистые функции Decide + Evolve.
/// Максимально тестируемо — ни DI, ни IO (идея 253).
/// </summary>
public interface IDecider<TState, TCommand, TEvent>
    where TState : class
    where TCommand : class
    where TEvent : class
{
    TState Initial { get; }

    IEnumerable<TEvent> Decide(TState state, TCommand command);

    TState Evolve(TState state, TEvent @event);

    bool IsTerminal(TState state) => false;
}

/// <summary>
/// Раннер для Decider: загрузка истории → решение → запись (с optimistic concurrency).
/// </summary>
public sealed class DeciderRunner<TState, TCommand, TEvent>
    where TState : class
    where TCommand : class
    where TEvent : class
{
    private readonly IDecider<TState, TCommand, TEvent> _decider;
    private readonly IEventStore _store;
    private readonly IEventSerializer _serializer;
    private readonly string _streamType;

    public DeciderRunner(
        IDecider<TState, TCommand, TEvent> decider,
        IEventStore store,
        IEventSerializer serializer,
        string streamType)
    {
        _decider = decider;
        _store = store;
        _serializer = serializer;
        _streamType = streamType;
    }

    public async ValueTask<AppendResult> HandleAsync(
        Guid streamId,
        TCommand command,
        CancellationToken ct = default)
    {
        var state = _decider.Initial;
        var version = 0;

        await foreach (var stored in _store.ReadStreamAsync(streamId, ct: ct))
        {
            var @event = (TEvent)_serializer.Deserialize(stored.Data, stored.EventType);
            state = _decider.Evolve(state, @event);
            version = stored.Version;
        }

        if (_decider.IsTerminal(state))
            return new AppendResult(version, 0, 0);

        var newEvents = _decider.Decide(state, command).ToList();
        if (newEvents.Count == 0)
            return new AppendResult(version, 0, 0);

        var toAppend = newEvents.Select(e => new EventToAppend
        {
            Payload = e,
            EventType = MessageTypeNaming.NameOf(e.GetType()),
        }).ToList();

        return await _store.AppendAsync(streamId, _streamType, toAppend, version, ct);
    }
}
