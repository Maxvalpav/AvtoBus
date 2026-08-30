using System.Diagnostics;
using AvtoBus.Observability;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.EventSourcing.Projections;

/// <summary>
/// Фоновый обработчик async-проекций: читает глобальный поток, применяет, чекпоинтит (идея 254).
/// Запускается как HostedService. Каждая проекция движется независимо со своим чекпоинтом.
/// </summary>
public sealed class ProjectionDaemon : BackgroundService
{
    private readonly IEventStore _store;
    private readonly IEnumerable<IProjection> _projections;
    private readonly IEventSerializer _serializer;
    private readonly UpcasterChain _upcasters;
    private readonly ProjectionDaemonOptions _options;
    private readonly ILogger<ProjectionDaemon> _log;

    public ProjectionDaemon(
        IEventStore store,
        IEnumerable<IProjection> projections,
        IEventSerializer serializer,
        UpcasterChain upcasters,
        ProjectionDaemonOptions options,
        ILogger<ProjectionDaemon> log)
    {
        _store = store;
        _projections = projections.Where(p => p.Mode == ProjectionMode.Async).ToList();
        _serializer = serializer;
        _upcasters = upcasters;
        _options = options;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var tasks = _projections.Select(p => RunProjectionAsync(p, ct));
        await Task.WhenAll(tasks);
    }

    private async Task RunProjectionAsync(IProjection projection, CancellationToken ct)
    {
        _log.LogInformation("Projection {Name} starting", projection.Name);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var checkpoint = await projection.GetCheckpointAsync(ct);
                var head = await _store.GetHeadSequenceAsync(ct);
                var lag = head - checkpoint;
                BusTelemetry.ProjectionLag.Record(lag, new KeyValuePair<string, object?>("projection", projection.Name));

                if (lag == 0)
                {
                    await Task.Delay(_options.IdleDelay, ct);
                    continue;
                }

                var processed = 0;
                var lastPosition = checkpoint;

                await foreach (var stored in _store.ReadAllAsync(
                    checkpoint, _options.BatchSize,
                    projection.HandledEventTypes.Count > 0 ? projection.HandledEventTypes : null,
                    ct))
                {
                    var @event = _upcasters.Upcast(
                        _serializer.Deserialize(stored.Data, stored.EventType),
                        stored.EventType, stored.SchemaVersion);

                    await projection.ApplyAsync(stored, @event, ct);

                    lastPosition = stored.GlobalSequence;
                    processed++;

                    if (processed >= _options.BatchSize) break;
                }

                if (processed > 0)
                {
                    await projection.SaveCheckpointAsync(lastPosition, ct);
                    _log.LogDebug("Projection {Name}: applied {Count} events, position {Pos}",
                        projection.Name, processed, lastPosition);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Projection {Name} failed, retrying in {Delay}", projection.Name, _options.ErrorDelay);
                await Task.Delay(_options.ErrorDelay, ct);
            }
        }
    }
}

public sealed class ProjectionDaemonOptions
{
    public int BatchSize { get; set; } = 500;
    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromMilliseconds(250);
    public TimeSpan ErrorDelay { get; set; } = TimeSpan.FromSeconds(5);
}
