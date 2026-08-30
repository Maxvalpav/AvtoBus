# 🔧 Реализация: транспорты (InMemory, RabbitMQ, Kafka-скетч)

> **Design draft.** Транспорт считается реализованным только после conformance suite; приведённые фрагменты такую проверку не проходили.

## 1. Единый интерфейс транспорта

```csharp
public interface ITransport
{
    string Name { get; }

    ValueTask SendAsync(Envelope envelope, TransportDestination dest, CancellationToken ct);

    IAsyncEnumerable<TransportMessage> ReceiveAsync(TransportSubscription sub,
        [EnumeratorCancellation] CancellationToken ct);

    ValueTask CancelScheduledAsync(Guid token, CancellationToken ct) => ValueTask.CompletedTask;

    ValueTask CreateTopologyAsync(TopologyPlan plan, CancellationToken ct) => ValueTask.CompletedTask;
}

public sealed record TransportSubscription(
    string Queue,
    IReadOnlyList<string> Topics,
    int Prefetch,
    string ConsumerGroup);
```

## 2. InMemory-транспорт — полная семантика брокера

```csharp
// AvtoBus.InMemory/InMemoryTransport.cs
public sealed class InMemoryTransport : ITransport, IDisposable
{
    public string Name => "inmemory";

    private readonly ConcurrentDictionary<string, InMemoryQueue> _queues = new();
    private readonly ConcurrentDictionary<string, HashSet<string>> _bindings = new();
    private readonly DelayScheduler _scheduler = new();

    public ValueTask SendAsync(Envelope env, TransportDestination dest, CancellationToken ct)
    {
        if (env.DeliverAt is { } at && at > DateTimeOffset.UtcNow)
        {
            _scheduler.Schedule(at, () => DeliverAsync(env, dest));
            return default;
        }
        return DeliverAsync(env, dest);
    }

    private ValueTask DeliverAsync(Envelope env, TransportDestination dest)
    {
        if (dest.Kind == DestinationKind.Queue)
        {
            var q = _queues.GetOrAdd(dest.Address, _ => new InMemoryQueue());
            return q.EnqueueAsync(env);
        }

        // topic → fan-out по bindings
        if (_bindings.TryGetValue(dest.Address, out var queues))
        {
            foreach (var qname in queues)
                _queues.GetOrAdd(qname, _ => new InMemoryQueue()).EnqueueAsync(env);
        }
        return default;
    }

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription sub, [EnumeratorCancellation] CancellationToken ct)
    {
        var q = _queues.GetOrAdd(sub.Queue, _ => new InMemoryQueue());
        foreach (var topic in sub.Topics)
            _bindings.GetOrAdd(topic, _ => new HashSet<string>()).Add(sub.Queue);

        await foreach (var env in q.ConsumeAsync(sub.Prefetch, ct))
            yield return new TransportMessage(env, new InMemoryAck(q, env));
    }
}

internal sealed class InMemoryQueue
{
    private readonly Channel<Envelope> _channel = Channel.CreateUnbounded<Envelope>(
        new UnboundedChannelOptions { SingleReader = false, SingleWriter = false });

    private readonly ConcurrentQueue<Envelope> _inflight = new();

    public ValueTask EnqueueAsync(Envelope env) => _channel.Writer.WriteAsync(env);

    public async IAsyncEnumerable<Envelope> ConsumeAsync(int prefetch, [EnumeratorCancellation] CancellationToken ct)
    {
        var semaphore = new SemaphoreSlim(prefetch);
        while (!ct.IsCancellationRequested)
        {
            await semaphore.WaitAsync(ct);
            var env = await _channel.Reader.ReadAsync(ct);
            _inflight.Enqueue(env);
            yield return env;
        }
    }

    internal void Ack(Envelope _)      { /* drop from _inflight */ }
    internal void Requeue(Envelope e)  => _channel.Writer.TryWrite(e.WithAttempt(e.DeliveryAttempt + 1));
}

internal sealed class InMemoryAck(InMemoryQueue q, Envelope e) : IAckContext
{
    public ValueTask AckAsync(CancellationToken ct = default)                     { q.Ack(e); return default; }
    public ValueTask NackAsync(bool requeue = false, CancellationToken ct = default) { if (requeue) q.Requeue(e); return default; }
    public ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default)
    {
        Task.Delay(delay, ct).ContinueWith(_ => q.Requeue(e), TaskContinuationOptions.OnlyOnRanToCompletion);
        return default;
    }
}
```

## 3. RabbitMQ-транспорт

```csharp
// AvtoBus.RabbitMq/RabbitMqTransport.cs (упрощённый)
using RabbitMQ.Client;

public sealed class RabbitMqTransport : ITransport, IAsyncDisposable
{
    public string Name => "rabbitmq";

    private readonly IConnection _connection;
    private readonly ObjectPool<IChannel> _channelPool;
    private readonly RabbitMqOptions _opt;

    public RabbitMqTransport(RabbitMqOptions opt)
    {
        _opt = opt;
        var factory = new ConnectionFactory
        {
            Uri = new Uri(opt.ConnectionString),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            DispatchConsumersAsync = true,
            ConsumerDispatchConcurrency = opt.ConsumerDispatchConcurrency
        };
        _connection = factory.CreateConnection("avtobus");
        _channelPool = new DefaultObjectPool<IChannel>(new ChannelPoolPolicy(_connection));
    }

    public async ValueTask SendAsync(Envelope env, TransportDestination dest, CancellationToken ct)
    {
        var ch = _channelPool.Get();
        try
        {
            var props = ch.CreateBasicProperties();
            props.Persistent = true;
            props.MessageId   = env.MessageId.ToString();
            props.ContentType = env.ContentType;
            props.ContentEncoding = env.ContentEncoding;
            props.Timestamp   = new AmqpTimestamp(env.SentAt.ToUnixTimeSeconds());
            props.Priority    = env.Priority;
            props.CorrelationId = env.CorrelationId?.ToString();
            props.ReplyTo     = env.ReplyTo;
            props.Type        = env.MessageType;
            if (env.TimeToLive is { } ttl) props.Expiration = ((long)ttl.TotalMilliseconds).ToString();
            props.Headers ??= new Dictionary<string, object?>();
            foreach (var (k, v) in env.Headers) props.Headers[k] = v;
            if (env.TenantId is not null) props.Headers["x-avb-tenant"] = env.TenantId;
            if (env.TraceParent is not null) props.Headers["traceparent"] = env.TraceParent;

            var routingKey = env.PartitionKey ?? "";

            // Автотопология: exchange по имени destination
            switch (dest.Kind)
            {
                case DestinationKind.Queue:
                    await ch.BasicPublishAsync("", dest.Address, mandatory: true, props, env.Body, ct);
                    break;
                case DestinationKind.Topic:
                    await ch.BasicPublishAsync(dest.Address, routingKey, mandatory: true, props, env.Body, ct);
                    break;
            }

            // Publisher confirms (батчами — упрощение)
            await ch.WaitForConfirmsOrDieAsync(ct);
        }
        finally { _channelPool.Return(ch); }
    }

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription sub, [EnumeratorCancellation] CancellationToken ct)
    {
        var ch = await _connection.CreateChannelAsync();
        await ch.BasicQosAsync(0, (ushort)sub.Prefetch, global: false, ct);

        // Топология: quorum queue, bindings в topic-exchange
        await ch.QueueDeclareAsync(sub.Queue, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                ["x-queue-type"] = "quorum",
                ["x-delivery-limit"] = _opt.DeliveryLimit
            }, ct);

        foreach (var topic in sub.Topics)
        {
            await ch.ExchangeDeclareAsync(topic, "topic", durable: true, autoDelete: false, ct);
            await ch.QueueBindAsync(sub.Queue, topic, "#", cancellationToken: ct);
        }

        var channel = Channel.CreateBounded<TransportMessage>(sub.Prefetch);
        var consumer = new AsyncEventingBasicConsumer(ch);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            var env = new Envelope
            {
                MessageId    = Guid.Parse(ea.BasicProperties.MessageId ?? Guid.CreateVersion7().ToString()),
                MessageType  = ea.BasicProperties.Type ?? "unknown",
                Body         = ea.Body,
                ContentType  = ea.BasicProperties.ContentType ?? "application/json",
                ContentEncoding = ea.BasicProperties.ContentEncoding ?? "identity",
                Priority     = ea.BasicProperties.Priority,
                CorrelationId= ParseGuid(ea.BasicProperties.CorrelationId),
                ReplyTo      = ea.BasicProperties.ReplyTo,
                DeliveryAttempt = GetAttempt(ea.BasicProperties.Headers),
                TenantId     = HeaderAsString(ea.BasicProperties.Headers, "x-avb-tenant"),
                TraceParent  = HeaderAsString(ea.BasicProperties.Headers, "traceparent"),
                Headers      = ExtractHeaders(ea.BasicProperties.Headers),
            };

            var ack = new RabbitAck(ch, ea.DeliveryTag);
            await channel.Writer.WriteAsync(new TransportMessage(env, ack), ct);
        };

        await ch.BasicConsumeAsync(sub.Queue, autoAck: false, consumer, ct);

        try
        {
            await foreach (var m in channel.Reader.ReadAllAsync(ct)) yield return m;
        }
        finally { await ch.CloseAsync(); }
    }

    public async ValueTask DisposeAsync() => await _connection.CloseAsync();
}

internal sealed class RabbitAck(IChannel ch, ulong tag) : IAckContext
{
    public ValueTask AckAsync(CancellationToken ct = default) => ch.BasicAckAsync(tag, multiple: false, ct);
    public ValueTask NackAsync(bool requeue = false, CancellationToken ct = default)
        => ch.BasicNackAsync(tag, multiple: false, requeue, ct);
    public ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default)
    {
        // TTL-очередь orders.retry.<ms> с dead-letter обратно в orders — см. идею 87
        throw new NotImplementedException("use retry-queue topology");
    }
}
```

## 4. Автотопология retry/dead-letter (idea 87)

```csharp
public static class RetryTopology
{
    // Создаёт цепочку очередей: orders.retry.5s → 30s → 5m, каждая с TTL и dead-letter обратно в orders
    public static async Task DeclareAsync(IChannel ch, string queue, TimeSpan[] backoffs, string errorQueue)
    {
        await ch.QueueDeclareAsync(errorQueue, durable: true, exclusive: false, autoDelete: false);

        foreach (var (delay, i) in backoffs.Select((d, i) => (d, i)))
        {
            var retryQueue = $"{queue}.retry.{FormatDelay(delay)}";
            await ch.QueueDeclareAsync(retryQueue, durable: true, exclusive: false, autoDelete: false,
                arguments: new Dictionary<string, object?>
                {
                    ["x-message-ttl"] = (int)delay.TotalMilliseconds,
                    ["x-dead-letter-exchange"] = "",
                    ["x-dead-letter-routing-key"] = queue,
                });
        }
    }

    private static string FormatDelay(TimeSpan d) => d.TotalSeconds < 60
        ? $"{(int)d.TotalSeconds}s" : $"{(int)d.TotalMinutes}m";
}
```

## 5. Kafka-транспорт (скетч на Confluent.Kafka)

```csharp
public sealed class KafkaTransport : ITransport
{
    public string Name => "kafka";
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly KafkaOptions _opt;

    public KafkaTransport(KafkaOptions opt)
    {
        _opt = opt;
        var config = new ProducerConfig
        {
            BootstrapServers = opt.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
            CompressionType = CompressionType.Zstd,
            LingerMs = opt.LingerMs,
            BatchSize = opt.BatchSize,
            MaxInFlight = 5,
            TransactionalId = opt.ExactlyOnce ? $"avtobus-{Environment.MachineName}" : null,
        };
        _producer = new ProducerBuilder<byte[], byte[]>(config).Build();
        if (opt.ExactlyOnce) _producer.InitTransactions(TimeSpan.FromSeconds(10));
    }

    public async ValueTask SendAsync(Envelope env, TransportDestination dest, CancellationToken ct)
    {
        var msg = new Message<byte[], byte[]>
        {
            Key = env.PartitionKey is null ? null : Encoding.UTF8.GetBytes(env.PartitionKey),
            Value = env.Body.ToArray(),
            Timestamp = new Timestamp(env.SentAt.UtcDateTime),
            Headers = BuildHeaders(env),
        };
        await _producer.ProduceAsync(dest.Address, msg, ct);
    }

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription sub, [EnumeratorCancellation] CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _opt.BootstrapServers,
            GroupId = sub.ConsumerGroup,
            EnableAutoCommit = false,
            IsolationLevel = _opt.ExactlyOnce ? IsolationLevel.ReadCommitted : IsolationLevel.ReadUncommitted,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        };
        using var consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
        consumer.Subscribe(sub.Topics);

        while (!ct.IsCancellationRequested)
        {
            var result = await Task.Run(() => consumer.Consume(ct), ct);
            var env = BuildEnvelopeFromKafka(result);
            yield return new TransportMessage(env, new KafkaAck(consumer, result));
        }
    }
}
```

## 6. Bus Host — принимает и запускает пайплайн

```csharp
internal sealed class BusHost : BackgroundService
{
    private readonly IEnumerable<ITransport> _transports;
    private readonly BusPipelineBuilder _pipelineBuilder;
    private readonly DispatcherRegistry _dispatchers;
    private readonly ISerializer _serializer;
    private readonly ISubscriptionCatalog _catalog;
    private readonly ILogger<BusHost> _log;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var pipeline = _pipelineBuilder.Build(_ => default);

        var tasks = _catalog.Subscriptions.Select(sub => Task.Run(async () =>
        {
            var transport = _transports.First(t => t.Name == sub.TransportName);

            await foreach (var msg in transport.ReceiveAsync(sub.ToTransportSubscription(), ct))
            {
                _ = ProcessAsync(msg, pipeline, ct);
            }
        }, ct));

        await Task.WhenAll(tasks);
    }

    private async Task ProcessAsync(TransportMessage msg, BusDelegate pipeline, CancellationToken ct)
    {
        try
        {
            if (!_dispatchers.TryGet(msg.Envelope.MessageType, out var dispatcher))
            {
                await msg.Ack.NackAsync(requeue: false, ct);          // → poison
                return;
            }

            var payload = _serializer.Deserialize(msg.Envelope.Body, dispatcher.ClrType);
            var ctx = new ConsumeContext
            {
                Envelope = msg.Envelope,
                Message = payload,
                Services = null!,          // подставится в ScopeMiddleware
                CancellationToken = ct
            };

            await pipeline(ctx);
            await msg.Ack.AckAsync(ct);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            _log.LogError(ex, "Consume failed for {Type}", msg.Envelope.MessageType);
            // Recoverability middleware обычно решает исход,
            // сюда попадаем только если он бросил дальше — → nack без requeue
            await msg.Ack.NackAsync(requeue: false, ct);
        }
    }
}
```

## 7. Conformance-kit транспорта (идея 98)

Каждый транспорт обязан пройти этот сьют:

```csharp
public abstract class TransportConformanceTests
{
    protected abstract Task<ITransport> CreateAsync();

    [Fact] public async Task Send_then_Receive_delivers_same_envelope() { /* ... */ }
    [Fact] public async Task Delivery_is_at_least_once_on_process_crash() { /* ... */ }
    [Fact] public async Task Priority_higher_delivered_first() { /* ... */ }
    [Fact] public async Task DelayedMessage_delivered_after_deadline() { /* ... */ }
    [Fact] public async Task Ttl_expired_message_is_dead_lettered() { /* ... */ }
    [Fact] public async Task PartitionKey_preserves_order_within_key() { /* ... */ }
    [Fact] public async Task Concurrent_consumers_do_not_double_process() { /* ... */ }
    [Fact] public async Task Nack_without_requeue_moves_to_dlq() { /* ... */ }
    [Fact] public async Task Headers_are_preserved_end_to_end() { /* ... */ }
    [Fact] public async Task Backpressure_no_OOM_under_flood() { /* ... */ }
    // ... ~80 сценариев
}

public sealed class RabbitMqConformanceTests : TransportConformanceTests
{
    protected override async Task<ITransport> CreateAsync() =>
        new RabbitMqTransport(await RabbitFixture.Start());
}
```

Так добавление нового транспорта — не риск, а понятный чек-лист.
