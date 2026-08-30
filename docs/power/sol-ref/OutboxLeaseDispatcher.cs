using System.Security.Cryptography;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using AvtoBus.Abstractions;

namespace AvtoBus.Persistence.Postgres;

public sealed class OutboxLeaseDispatcher : BackgroundService
{
    private readonly PostgresOutboxLeaseStore _store;
    private readonly ITransportPublisher _publisher;
    private readonly IOutboxSignal _signal;
    private readonly PostgresAvtoBusOptions _options;
    private readonly AvtoBus.Core.AvtoBusMetrics? _metrics;
    private readonly ILogger<OutboxLeaseDispatcher> _logger;

    public OutboxLeaseDispatcher(
        PostgresOutboxLeaseStore store,
        ITransportPublisher publisher,
        IOutboxSignal signal,
        IOptions<PostgresAvtoBusOptions> options,
        ILogger<OutboxLeaseDispatcher> logger,
        AvtoBus.Core.AvtoBusMetrics? metrics = null)
    {
        _store = store;
        _publisher = publisher;
        _signal = signal;
        _options = options.Value;
        _metrics = metrics;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var items = await _store.ClaimAsync(stoppingToken);
                if (items.Count == 0)
                {
                    await WaitForSignalOrDelayAsync(stoppingToken);
                    continue;
                }

                await Parallel.ForEachAsync(
                    items,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = _options.MaxPublishConcurrency,
                        CancellationToken = stoppingToken
                    },
                    PublishOneAsync);

                if (items.Count < _options.BatchSize)
                    await WaitForSignalOrDelayAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Outbox dispatch loop failed.");
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }
    }

    private async ValueTask PublishOneAsync(
        LeasedOutboxMessage item,
        CancellationToken cancellationToken)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            using var activity = AvtoBus.Core.AvtoBusActivity.StartOutboxDispatch(item.Destination);
            var actualHash = SHA256.HashData(item.Envelope);
            if (!CryptographicOperations.FixedTimeEquals(
                    actualHash, item.EnvelopeSha256))
            {
                _metrics?.IncrementSignatureFailures("outbox_envelope_hash_mismatch");
                await _store.HandleFailureAsync(
                    item,
                    "outbox_envelope_hash_mismatch",
                    exception: null,
                    cancellationToken: cancellationToken,
                    permanent: true,
                    securityRisk: true);
                return;
            }

            var transportMessage = new TransportMessage(
                item.Envelope,
                item.ContentType,
                item.Destination,
                item.PartitionKey,
                item.TransportHeaders);

            var result = await _publisher.PublishAsync(
                transportMessage, cancellationToken);

            if (!result.Confirmed)
            {
                _metrics?.IncrementOutboxFailures(result.ErrorCode ?? "broker_not_confirmed");
                await _store.HandleFailureAsync(
                    item,
                    result.ErrorCode ?? "broker_not_confirmed",
                    result.ErrorMessage is null
                        ? null
                        : new TransportPublishException(result.ErrorMessage),
                    cancellationToken);
                return;
            }

            _metrics?.RecordOutboxPublishDuration(sw.Elapsed, item.Destination);
            var marked = await _store.MarkSentAsync(item, cancellationToken);
            if (!marked)
            {
                _metrics?.IncrementLeaseLost(item.LockedBy);
                _logger.LogWarning(
                    "Event {EventId} was confirmed by broker, but lease was lost before MarkSent. " +
                    "A duplicate publication is possible.",
                    item.EventId);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Lease истечет и сообщение будет повторено другим воркером.
        }
        catch (Exception exception)
        {
            _metrics?.IncrementOutboxFailures("transport_exception");
            await _store.HandleFailureAsync(
                item, "transport_exception", exception, cancellationToken);
        }
    }

    private async Task WaitForSignalOrDelayAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var signalTask = _signal.WaitAsync(linked.Token).AsTask();
        var delayTask = Task.Delay(_options.IdlePollingInterval, linked.Token);

        try
        {
            await Task.WhenAny(signalTask, delayTask);
        }
        finally
        {
            await linked.CancelAsync();
            try { await Task.WhenAll(signalTask, delayTask); }
            catch (OperationCanceledException) { }
        }
    }
}
