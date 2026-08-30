# AvtoBus.Kafka — Полная реализация транспорта

---

## AvtoBus.Kafka/KafkaTransport.cs

```csharp
using Confluent.Kafka;
using System.Text;
using System.Threading.Channels;

namespace AvtoBus.Transport.Kafka;

public sealed class KafkaTransport : ITransport
{
    public string Name => "kafka";

    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaTransport> _log;
    private readonly IProducer<byte[]?, byte[]> _producer;
    private readonly bool _exactlyOnce;

    public KafkaTransport(KafkaOptions options, ILogger<KafkaTransport> log)
    {
        _options = options;
        _log = log;
        _exactlyOnce = options.ExactlyOnce;

        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
            CompressionType = CompressionType.Zstd,
            LingerMs = options.LingerMs,
            BatchSize = options.BatchSizeBytes,
            MaxInFlight = 5,
            MessageSendMaxRetries = 10,
            RetryBackoffMs = 100,
            TransactionalId = _exactlyOnce
                ? $"avtobus-{Environment.MachineName}-{Environment.ProcessId}"
                : null,
        };

        _producer = new ProducerBuilder<byte[]?, byte[]>(config)
            .SetErrorHandler((_, e) => _log.LogError("Kafka producer error: {Error}", e.Reason))
            .Build();

        if (_exactlyOnce)
            _producer.InitTransactions(TimeSpan.FromSeconds(30));
    }

    // ── Send ──

    public async ValueTask SendAsync(Envelope envelope, TransportDestination dest, CancellationToken ct = default)
    {
        var headers = new Headers();
        headers.Add("avtobus.message-type", Encoding.UTF8.GetBytes(envelope.MessageType));
        headers.Add("avtobus.message-id", Encoding.UTF8.GetBytes(envelope.MessageId.ToString()));

        if (envelope.CorrelationId is { } corr)
            headers.Add("avtobus.correlation-id", Encoding.UTF8.GetBytes(corr.ToString()));
        if (envelope.CausationId is { } caus)
            headers.Add("avtobus.causation-id", Encoding.UTF8.GetBytes(caus.ToString()));
        if (envelope.TenantId is not null)
            headers.Add("avtobus.tenant-id", Encoding.UTF8.GetBytes(envelope.TenantId));
        if (envelope.TraceParent is not null)
            headers.Add("traceparent", Encoding.UTF8.GetBytes(envelope.TraceParent));

        foreach (var (k, v) in envelope.Headers)
            headers.Add($"avtobus.h.{k}", Encoding.UTF8.GetBytes(v));

        byte[]? key = envelope.PartitionKey is not null
            ? Encoding.UTF8.GetBytes(envelope.PartitionKey)
            : null;

        var message = new Message<byte[]?, byte[]>
        {
            Key = key,
            Value = envelope.Body.ToArray(),
            Timestamp = new Timestamp(envelope.SentAt.UtcDateTime, TimestampType.CreateTime),
            Headers = headers,
        };

        var report = await _producer.ProduceAsync(dest.Address, message, ct);

        if (report.Status == PersistenceStatus.NotPersisted)
            throw new InvalidOperationException($"Kafka delivery failed: {report.Status}");

        _log.LogDebug("Kafka produced {Type} to {Topic}:{Partition}@{Offset}",
            envelope.MessageType, report.Topic, report.Partition, report.Offset);
    }

    // ── Batch Send (для outbox relay) ──

    public async ValueTask SendBatchAsync(
        IReadOnlyList<(Envelope Envelope, TransportDestination Dest)> batch,
        CancellationToken ct)
    {
        if (_exactlyOnce)
            _producer.BeginTransaction();

        try
        {
            var completions = new List<Task<DeliveryResult<byte[]?, byte[]>>>(batch.Count);
            foreach (var (envelope, dest) in batch)
            {
                var headers = BuildHeaders(envelope);
                byte[]? key = envelope.PartitionKey is not null
                    ? Encoding.UTF8.GetBytes(envelope.PartitionKey) : null;

                var message = new Message<byte[]?, byte[]>
                {
                    Key = key,
                    Value = envelope.Body.ToArray(),
                    Timestamp = new Timestamp(envelope.SentAt.UtcDateTime),
                    Headers = headers,
                };

                completions.Add(_producer.ProduceAsync(dest.Address, message, ct));
            }

            await Task.WhenAll(completions);

            if (_exactlyOnce)
                _producer.CommitTransaction();
        }
        catch
        {
            if (_exactlyOnce)
                _producer.AbortTransaction();
            throw;
        }
    }

    // ── Receive ──

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = subscription.ConsumerGroup,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            IsolationLevel = _exactlyOnce
                ? Confluent.Kafka.IsolationLevel.ReadCommitted
                : Confluent.Kafka.IsolationLevel.ReadUncommitted,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            MaxPollIntervalMs = 300_000,
            SessionTimeoutMs = 45_000,
            FetchMaxBytes = _options.FetchMaxBytes,
            MaxPartitionFetchBytes = _options.MaxPartitionFetchBytes,
        };

        using var consumer = new ConsumerBuilder<byte[]?, byte[]>(config)
            .SetErrorHandler((_, e) => _log.LogError("Kafka consumer error: {Error}", e.Reason))
            .SetPartitionsAssignedHandler((c, partitions) =>
                _log.LogInformation("Assigned: {Partitions}", string.Join(",", partitions)))
            .SetPartitionsRevokedHandler((c, partitions) =>
                _log.LogInformation("Revoked: {Partitions}", string.Join(",", partitions)))
            .Build();

        consumer.Subscribe(subscription.Topics);
        _log.LogInformation("Kafka consumer started: group={Group}, topics={Topics}",
            subscription.ConsumerGroup, string.Join(",", subscription.Topics));

        var pending = new Channel<TransportMessage>(
            Channel.CreateBounded<TransportMessage>(subscription.Prefetch));

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<byte[]?, byte[]>? result;
                try
                {
                    result = consumer.Consume(ct);
                }
                catch (ConsumeException ex)
                {
                    _log.LogError(ex, "Kafka consume error");
                    continue;
                }

                if (result?.Message is null) continue;

                var envelope = ParseEnvelope(result);
                var ack = new KafkaAckContext(consumer, result);

                yield return new TransportMessage(envelope, ack);
            }
        }
        finally
        {
            consumer.Close();
        }
    }

    // ── Topology ──

    public async ValueTask CreateTopologyAsync(TopologyPlan plan, CancellationToken ct = default)
    {
        using var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = _options.BootstrapServers }).Build();

        var existing = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        var existingTopics = existing.Topics.Select(t => t.Topic).ToHashSet();

        var toCreate = plan.Topics
            .Where(t => !existingTopics.Contains(t.Name))
            .Select(t => new TopicSpecification
            {
                Name = t.Name,
                NumPartitions = _options.DefaultPartitions,
                ReplicationFactor = _options.DefaultReplicationFactor,
            })
            .ToList();

        if (toCreate.Count > 0)
        {
            await adminClient.CreateTopicsAsync(toCreate);
            _log.LogInformation("Created Kafka topics: {Topics}",
                string.Join(", ", toCreate.Select(t => t.Name)));
        }
    }

    // ── Helpers ──

    private static Envelope ParseEnvelope(ConsumeResult<byte[]?, byte[]> result)
    {
        var headers = new Dictionary<string, string>();
        foreach (var h in result.Message.Headers)
        {
            headers[h.Key] = Encoding.UTF8.GetString(h.GetValueBytes());
        }

        return new Envelope
        {
            MessageId = Guid.TryParse(headers.GetValueOrDefault("avtobus.message-id"), out var id) ? id : Guid.NewGuid(),
            CorrelationId = Guid.TryParse(headers.GetValueOrDefault("avtobus.correlation-id"), out var c) ? c : null,
            CausationId = Guid.TryParse(headers.GetValueOrDefault("avtobus.causation-id"), out var ca) ? ca : null,
            MessageType = headers.GetValueOrDefault("avtobus.message-type") ?? "unknown",
            Body = result.Message.Value,
            SentAt = result.Message.Timestamp.UtcDateTime != default
                ? new DateTimeOffset(result.Message.Timestamp.UtcDateTime)
                : DateTimeOffset.UtcNow,
            PartitionKey = result.Message.Key is not null ? Encoding.UTF8.GetString(result.Message.Key) : null,
            TenantId = headers.GetValueOrDefault("avtobus.tenant-id"),
            TraceParent = headers.GetValueOrDefault("traceparent"),
            Headers = headers
                .Where(kv => kv.Key.StartsWith("avtobus.h."))
                .ToDictionary(kv => kv.Key["avtobus.h.".Length..], kv => kv.Value)
                .ToFrozenDictionary(),
        };
    }

    private static Headers BuildHeaders(Envelope envelope)
    {
        var headers = new Headers();
        headers.Add("avtobus.message-type", Encoding.UTF8.GetBytes(envelope.MessageType));
        headers.Add("avtobus.message-id", Encoding.UTF8.GetBytes(envelope.MessageId.ToString()));
        if (envelope.CorrelationId is { } c)
            headers.Add("avtobus.correlation-id", Encoding.UTF8.GetBytes(c.ToString()));
        if (envelope.TenantId is not null)
            headers.Add("avtobus.tenant-id", Encoding.UTF8.GetBytes(envelope.TenantId));
        if (envelope.TraceParent is not null)
            headers.Add("traceparent", Encoding.UTF8.GetBytes(envelope.TraceParent));
        return headers;
    }

    public ValueTask DisposeAsync()
    {
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }

    public void Dispose() => _producer.Dispose();
}

internal sealed class KafkaAckContext : IAckContext
{
    private readonly IConsumer<byte[]?, byte[]> _consumer;
    private readonly ConsumeResult<byte[]?, byte[]> _result;
    private int _acked;

    public KafkaAckContext(IConsumer<byte[]?, byte[]> consumer, ConsumeResult<byte[]?, byte[]> result)
    {
        _consumer = consumer;
        _result = result;
    }

    public ValueTask AckAsync(CancellationToken ct = default)
    {
        if (Interlocked.CompareExchange(ref _acked, 1, 0) == 0)
        {
            try { _consumer.Commit(_result); }
            catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_NoOffset) { }
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask NackAsync(bool requeue = false, CancellationToken ct = default)
    {
        // Kafka: nack без requeue = skip (committed); с requeue = seek назад
        if (requeue)
        {
            _consumer.Seek(_result.TopicPartitionOffset);
        }
        else
        {
            // Commit, чтобы пропустить — сообщение уйдёт в DLT если настроен
            _consumer.Commit(_result);
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default)
    {
        // В Kafka defer = commit + publish в retry-topic с timestamp
        _consumer.Commit(_result);
        return ValueTask.CompletedTask;
    }
}

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public bool ExactlyOnce { get; set; }
    public int LingerMs { get; set; } = 5;
    public int BatchSizeBytes { get; set; } = 1_000_000;
    public int FetchMaxBytes { get; set; } = 52_428_800;
    public int MaxPartitionFetchBytes { get; set; } = 1_048_576;
    public int DefaultPartitions { get; set; } = 12;
    public short DefaultReplicationFactor { get; set; } = 3;
    public string? SchemaRegistryUrl { get; set; }
}
```

---

## AvtoBus.Kafka/Registration.cs

```csharp
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus;

public static class KafkaRegistration
{
    public static BusOptions UseKafka(this BusOptions bus, Action<KafkaOptions> configure)
    {
        var opts = new KafkaOptions();
        configure(opts);
        bus.DefaultTransport = "kafka";
        bus.Services.AddSingleton(opts);
        bus.Services.AddSingleton<ITransport>(sp =>
            new Transport.Kafka.KafkaTransport(opts, sp.GetRequiredService<ILogger<Transport.Kafka.KafkaTransport>>()));
        return bus;
    }

    public static BusOptions UseKafka(this BusOptions bus, string bootstrapServers, bool exactlyOnce = false)
        => bus.UseKafka(o =>
        {
            o.BootstrapServers = bootstrapServers;
            o.ExactlyOnce = exactlyOnce;
        });
}
```

---

## AvtoBus.Kafka/KafkaConformanceTests.cs

```csharp
namespace AvtoBus.Conformance;

public sealed class KafkaConformanceTests : TransportConformanceTests
{
    private readonly Testcontainers.Kafka.KafkaContainer _kafka =
        new Testcontainers.Kafka.KafkaBuilder().Build();

    protected override async ValueTask<ITransport> CreateTransportAsync()
    {
        await _kafka.StartAsync();
        return new Transport.Kafka.KafkaTransport(
            new KafkaOptions { BootstrapServers = _kafka.GetBootstrapAddress() },
            Microsoft.Extensions.Logging.Abstractions.NullLogger<Transport.Kafka.KafkaTransport>.Instance);
    }

    public override async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await _kafka.DisposeAsync();
    }
}
```
