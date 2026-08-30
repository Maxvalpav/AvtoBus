using System.Security.Cryptography;
using AvtoBus.Core;
using AvtoBus.Core.Security;
using AvtoBus.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace AvtoBus.Persistence.Postgres;

public sealed class ConsumerHostedService : BackgroundService
{
    private readonly ITransportReceiver _receiver;
    private readonly IMessageRegistry _messages;
    private readonly IConsumerDispatcherRegistry _dispatchers;
    private readonly PostgresInboxStore _inbox;
    private readonly PostgresDlqStore _dlq;
    private readonly NpgsqlDataSource _dataSource;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IMessageSecurity _security;
    private readonly EventAuthorizationPolicy _authorization;
    private readonly PostgresAvtoBusOptions _options;
    private readonly MessageSecurityLimits _securityLimits;
    private readonly AvtoBusMetrics? _metrics;
    private readonly ILogger<ConsumerHostedService> _logger;

    public ConsumerHostedService(
        ITransportReceiver receiver,
        IMessageRegistry messages,
        IConsumerDispatcherRegistry dispatchers,
        PostgresInboxStore inbox,
        PostgresDlqStore dlq,
        NpgsqlDataSource dataSource,
        IServiceScopeFactory scopeFactory,
        IMessageSecurity security,
        EventAuthorizationPolicy authorization,
        IOptions<PostgresAvtoBusOptions> options,
        ILogger<ConsumerHostedService> logger,
        AvtoBusMetrics? metrics = null,
        MessageSecurityLimits? securityLimits = null)
    {
        _receiver = receiver;
        _messages = messages;
        _dispatchers = dispatchers;
        _inbox = inbox;
        _dlq = dlq;
        _dataSource = dataSource;
        _scopeFactory = scopeFactory;
        _security = security;
        _authorization = authorization;
        _options = options.Value;
        _metrics = metrics;
        _securityLimits = securityLimits ?? new MessageSecurityLimits();
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var concurrency = Math.Max(1, _options.MaxConsumerConcurrency);
        var channel = System.Threading.Channels.Channel.CreateBounded<IReceivedTransportMessage>(
            new System.Threading.Channels.BoundedChannelOptions(concurrency * 2)
            {
                SingleWriter = true,
                SingleReader = false,
                FullMode = System.Threading.Channels.BoundedChannelFullMode.Wait
            });

        var producer = Task.Run(async () =>
        {
            await foreach (var msg in _receiver.ReadAllAsync(stoppingToken))
                await channel.Writer.WriteAsync(msg, stoppingToken);
            channel.Writer.Complete();
        }, stoppingToken);

        var consumers = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
        {
            await foreach (var received in channel.Reader.ReadAllAsync(stoppingToken))
                await ProcessOneAsync(received, stoppingToken);
        }, stoppingToken)).ToArray();

        try
        {
            await producer;
            await Task.WhenAll(consumers);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            channel.Writer.TryComplete();
        }
    }

    private static void ValidateSecurityLimits(IReceivedTransportMessage received, PostgresAvtoBusOptions options, AvtoBus.Core.Security.MessageSecurityLimits limits)
    {
        if (received.Body.Length > options.MaxEnvelopeBytes)
            throw new PermanentMessageException("message_too_large", true);
        AvtoBus.Core.Security.SecurityValidator.ValidateEnvelope(received.Body.Span, received.Headers, limits);
    }

    private async ValueTask ProcessOneAsync(
        IReceivedTransportMessage received,
        CancellationToken cancellationToken)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        string? eventTypeForMetrics = null;
        string? consumerForMetrics = null;
        try
        {
            if (!StringComparer.OrdinalIgnoreCase.Equals(
                    received.ContentType, "application/cloudevents+json"))
                throw new PermanentMessageException("unsupported_content_type");

            ValidateSecurityLimits(received, _options, _securityLimits);

            await _security.VerifyAsync(
                received.Body, received.Headers, cancellationToken);

            var metadata = CloudEventHeaderReader.Read(received.Body.Span, _securityLimits.MaxJsonDepth);
            SecurityValidator.ValidateMetadata(metadata, _securityLimits);
            eventTypeForMetrics = metadata.Type;
            using var receiveActivity = AvtoBusActivity.StartReceive(metadata.Type, metadata.Id);
            var descriptor = _messages.GetByEventType(metadata.Type);
            var dispatcher = _dispatchers.Get(metadata.Type);
            consumerForMetrics = dispatcher.ConsumerName;
            _authorization.Authorize(dispatcher.ConsumerName, metadata);
            var hash = SHA256.HashData(received.Body.Span);
            var decoded = descriptor.Decode(received.Body.Span);

            await using var scope = _scopeFactory.CreateAsyncScope();
            await using var session = await AvtoBusDbSession.BeginAsync(
                _dataSource, cancellationToken: cancellationToken);

            var acquired = await _inbox.TryAcquireAsync(
                session,
                dispatcher.ConsumerName,
                metadata,
                hash,
                cancellationToken);

            if (!acquired)
            {
                _metrics?.IncrementInboxDuplicates(metadata.Type, dispatcher.ConsumerName);
                await session.CommitAsync(cancellationToken);
                await received.AckAsync(cancellationToken);
                return;
            }

            using var consumeActivity = AvtoBusActivity.StartConsume(dispatcher.ConsumerName, metadata.Type, DeliveryCount(received.Headers));
            await dispatcher.DispatchAsync(
                decoded,
                scope.ServiceProvider,
                session,
                cancellationToken);

            await session.CommitAsync(cancellationToken);
            await received.AckAsync(cancellationToken);
            _metrics?.RecordConsumerDuration(sw.Elapsed, metadata.Type, dispatcher.ConsumerName);
        }
        catch (PermanentMessageException exception)
        {
            if (exception.SecurityRisk) _metrics?.IncrementSignatureFailures(exception.Code);
            using var dlqActivity = AvtoBusActivity.StartDlq(exception.Code);
            await _dlq.StoreIncomingAsync(
                received,
                exception.Code,
                exception,
                exception.SecurityRisk,
                cancellationToken);
            await received.AckAsync(cancellationToken);
            if (eventTypeForMetrics is not null && consumerForMetrics is not null)
                _metrics?.IncrementConsumerFailures(eventTypeForMetrics, consumerForMetrics);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Transient consumer failure.");
            var deliveryCount = DeliveryCount(received.Headers);
            if (deliveryCount >= _options.MaxConsumerDeliveryAttempts)
            {
                await _dlq.StoreIncomingAsync(
                    received,
                    "consumer_attempts_exhausted",
                    exception,
                    securityRisk: false,
                    cancellationToken);
                await received.AckAsync(cancellationToken);
            }
            else
            {
                await received.NackAsync(requeue: true, cancellationToken);
            }
        }
    }

    private static int DeliveryCount(IReadOnlyDictionary<string, string> headers) =>
        headers.TryGetValue("avtobus-delivery-count", out var raw)
        && int.TryParse(raw, out var count)
            ? count
            : 1;
}
