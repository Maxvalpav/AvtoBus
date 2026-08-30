using AvtoBus.EventSourcing;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.EventSourcing.Projections;

/// <summary>
/// Подписка на события стора с публикацией в шину (идея 269, 265):
/// событие стора автоматически публикуется в брокер через outbox, начиная с чекпоинта подписки.
/// Correlation/Causation-цепочка сохраняется из метаданных события.
/// </summary>
public sealed class StoreEventSubscription : BackgroundService
{
    private readonly IEventStore _store;
    private readonly IEventSerializer _serializer;
    private readonly UpcasterChain _upcasters;
    private readonly IBus _bus;
    private readonly string _name;
    private readonly string? _streamTypeFilter;
    private readonly long _fromSequence;
    private readonly ILogger<StoreEventSubscription> _log;

    public StoreEventSubscription(
        IEventStore store,
        IEventSerializer serializer,
        UpcasterChain upcasters,
        IBus bus,
        StoreSubscriptionOptions options,
        ILogger<StoreEventSubscription> log)
    {
        _store = store;
        _serializer = serializer;
        _upcasters = upcasters;
        _bus = bus;
        _name = options.Name;
        _streamTypeFilter = options.StreamType;
        _fromSequence = options.FromSequence;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _log.LogInformation("Store subscription {Name} starting from sequence {From}",
            _name, _fromSequence);

        var position = _fromSequence;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var head = await _store.GetHeadSequenceAsync(ct);
                if (position >= head)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
                    continue;
                }

                var processed = 0;
                var lastPosition = position;

                await foreach (var stored in _streamTypeFilter is null
                                   ? _store.ReadAllAsync(position, 100, ct: ct)
                                   : _store.ReadCategoryAsync(_streamTypeFilter, position, ct))
                {
                    var @event = _upcasters.Upcast(
                        _serializer.Deserialize(stored.Data, stored.EventType),
                        stored.EventType, stored.SchemaVersion);

                    await _bus.PublishAsync(@event, new PublishOptions
                    {
                        PartitionKey = stored.StreamId.ToString(),
                        CorrelationId = stored.CorrelationId,
                        CausationId = stored.CausationId,
                        TenantId = stored.TenantId,
                    }, ct);

                    lastPosition = stored.GlobalSequence;
                    processed++;

                    if (processed >= 100) break;
                }

                if (processed > 0)
                {
                    position = lastPosition;
                    _log.LogDebug("Store subscription {Name}: published {Count}, position {Pos}",
                        _name, processed, lastPosition);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Store subscription {Name} failed, retrying", _name);
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }
}

public sealed class StoreSubscriptionOptions
{
    /// <summary>Имя подписки (для логов).</summary>
    public required string Name { get; init; }

    /// <summary>Фильтр по типу стрима ($ce-orders); null — все события.</summary>
    public string? StreamType { get; init; }

    /// <summary>Начинать с этой последовательности (по умолчанию с головы).</summary>
    public long FromSequence { get; init; } = 0;
}
