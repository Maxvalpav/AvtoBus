# AvtoBus.RabbitMq — Полная реализация транспорта

> **Code sketch / unverified.** Заголовок описывает целевой охват, а не подтверждённую готовность. Нужны RabbitMQ integration и conformance tests. Канонический статус: [`../FINAL.md`](../FINAL.md).

---

## AvtoBus.RabbitMq/RabbitMqTransport.cs

```csharp
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Channels;

namespace AvtoBus.Transport.RabbitMq;

/// <summary>
/// Транспорт RabbitMQ с quorum queues, publisher confirms, автотопологией.
/// </summary>
public sealed class RabbitMqTransport : ITransport
{
    public string Name => "rabbitmq";

    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqTransport> _log;
    private readonly IConnection _connection;
    private readonly SemaphoreSlim _publishLock = new(1, 1);
    private readonly ObjectPool<IChannel> _channelPool;

    public RabbitMqTransport(string connectionString, ILogger<RabbitMqTransport> log)
    {
        _log = log;
        _options = new RabbitMqOptions { ConnectionString = connectionString };

        var factory = new ConnectionFactory
        {
            Uri = new Uri(connectionString),
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            RequestedHeartbeat = TimeSpan.FromSeconds(30),
            ConsumerDispatchConcurrency = 4,
        };

        _connection = factory.CreateConnection("avtobus");
        _channelPool = new DefaultObjectPool<IChannel>(
            new ChannelPoolPolicy(_connection), 16);

        _log.LogInformation("RabbitMQ connection established: {Endpoint}", connectionString);
    }

    // ── Send ──

    public async ValueTask SendAsync(
        Envelope envelope,
        TransportDestination dest,
        CancellationToken ct = default)
    {
        var channel = _channelPool.Get();
        try
        {
            var properties = new BasicProperties
            {
                Persistent = true,
                MessageId = envelope.MessageId.ToString(),
                ContentType = envelope.ContentType,
                ContentEncoding = envelope.ContentEncoding,
                CorrelationId = envelope.CorrelationId?.ToString(),
                ReplyTo = envelope.ReplyTo,
                Type = envelope.MessageType,
                Priority = envelope.Priority,
                Timestamp = new AmqTimestamp(envelope.SentAt.ToUnixTimeMilliseconds()),
                Headers = new Dictionary<string, object?>()
            };

            if (envelope.TimeToLive is { } ttl)
                properties.Expiration = ((long)ttl.TotalMilliseconds).ToString();

            // Добавляем headers
            foreach (var (key, value) in envelope.Headers)
                properties.Headers![key] = Encoding.UTF8.GetBytes(value);

            if (envelope.TenantId is not null)
                properties.Headers!["x-avb-tenant"] = Encoding.UTF8.GetBytes(envelope.TenantId);

            if (envelope.TraceParent is not null)
                properties.Headers!["traceparent"] = Encoding.UTF8.GetBytes(envelope.TraceParent);

            properties.Headers!["avtobus.message-type"] = Encoding.UTF8.GetBytes(envelope.MessageType);

            var routingKey = envelope.PartitionKey ?? "";

            switch (dest.Kind)
            {
                case DestinationKind.Queue:
                    channel.BasicPublish(
                        exchange: "",
                        routingKey: dest.Address,
                        mandatory: true,
                        basicProperties: properties,
                        body: envelope.Body.ToArray());
                    break;

                case DestinationKind.Topic:
                    channel.BasicPublish(
                        exchange: dest.Address,
                        routingKey: routingKey,
                        mandatory: true,
                        basicProperties: properties,
                        body: envelope.Body.ToArray());
                    break;
            }

            await channel.WaitForConfirmsOrDieAsync(ct);

            _log.LogDebug("Published {Type} to {Destination}", envelope.MessageType, dest.Address);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Failed to publish {Type}", envelope.MessageType);
            throw;
        }
        finally
        {
            _channelPool.Return(channel);
        }
    }

    // ── Receive ──

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var channel = await _connection.CreateChannelAsync();
        await channel.BasicQosAsync(0, (ushort)subscription.Prefetch, global: false, ct);

        // Declare quorum queue
        var args = new Dictionary<string, object?>
        {
            ["x-queue-type"] = "quorum",
            ["x-delivery-limit"] = 6,
        };

        await channel.QueueDeclareAsync(
            queue: subscription.Queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: args,
            cancellationToken: ct);

        // Bind to topics
        foreach (var topic in subscription.Topics)
        {
            await channel.ExchangeDeclareAsync(
                exchange: topic,
                type: "topic",
                durable: true,
                cancellationToken: ct);

            await channel.QueueBindAsync(
                queue: subscription.Queue,
                exchange: topic,
                routingKey: "#",
                cancellationToken: ct);
        }

        var messageChannel = Channel.CreateBounded<TransportMessage>(
            new BoundedChannelOptions(subscription.Prefetch)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
            });

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                var envelope = ParseEnvelope(ea);
                var ackContext = new RabbitAckContext(channel, ea.DeliveryTag);
                var msg = new TransportMessage(envelope, ackContext);

                await messageChannel.Writer.WriteAsync(msg, ct);
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Error processing message from {Queue}", subscription.Queue);
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
            }
        };

        consumer.ShutdownAsync += (_, args) =>
        {
            messageChannel.Writer.Complete();
            return Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(
            queue: subscription.Queue,
            autoAck: false,
            consumer: consumer,
            cancellationToken: ct);

        try
        {
            await foreach (var msg in messageChannel.Reader.ReadAllAsync(ct))
                yield return msg;
        }
        finally
        {
            try { await channel.CloseAsync(); } catch { }
            channel.Dispose();
        }
    }

    // ── Topology ──

    public async ValueTask CreateTopologyAsync(
        TopologyPlan plan,
        CancellationToken ct = default)
    {
        var channel = await _connection.CreateChannelAsync();
        try
        {
            foreach (var queue in plan.Queues)
            {
                var args = new Dictionary<string, object?>();

                if (queue.TimeToLive is { } ttl)
                    args["x-message-ttl"] = (long)ttl.TotalMilliseconds;

                await channel.QueueDeclareAsync(
                    queue.Name, queue.Durable, queue.Exclusive,
                    queue.AutoDelete, args, ct);
            }

            foreach (var topic in plan.Topics)
            {
                await channel.ExchangeDeclareAsync(
                    topic.Name, topic.Type, topic.Durable, cancellationToken: ct);
            }

            foreach (var binding in plan.Bindings)
            {
                await channel.QueueBindAsync(
                    queue: binding.Destination,
                    exchange: binding.Source,
                    routingKey: binding.RoutingKey,
                    cancellationToken: ct);
            }

            _log.LogInformation("Topology applied: {Queues} queues, {Topics} topics, {Bindings} bindings",
                plan.Queues.Count, plan.Topics.Count, plan.Bindings.Count);
        }
        finally
        {
            channel.Dispose();
        }
    }

    // ── Dispose ──

    public async ValueTask DisposeAsync()
    {
        _channelPool?.Dispose();
        if (_connection.IsOpen)
            await _connection.CloseAsync();
        _connection.Dispose();
    }

    public void Dispose()
    {
        _connection.Dispose();
    }

    // ── Private helpers ──

    private static Envelope ParseEnvelope(BasicDeliverEventArgs ea)
    {
        var headers = new Dictionary<string, string>();
        if (ea.BasicProperties.Headers is not null)
        {
            foreach (var (key, value) in ea.BasicProperties.Headers)
            {
                if (value is byte[] bytes)
                    headers[key] = Encoding.UTF8.GetString(bytes);
                else if (value is not null)
                    headers[key] = value.ToString() ?? "";
            }
        }

        return new Envelope
        {
            MessageId = Guid.Parse(ea.BasicProperties.MessageId ?? Guid.NewGuid().ToString()),
            CorrelationId = ParseGuid(ea.BasicProperties.CorrelationId),
            CausationId = ParseGuid(headers.GetValueOrDefault("avtobus.causation-id")),
            MessageType = ea.BasicProperties.Type ?? headers.GetValueOrDefault("avtobus.message-type") ?? "unknown",
            Body = ea.Body,
            ContentType = ea.BasicProperties.ContentType ?? "application/json",
            ContentEncoding = ea.BasicProperties.ContentEncoding ?? "identity",
            SentAt = ea.BasicProperties.Timestamp.UnixTime > 0
                ? DateTimeOffset.FromUnixTimeMilliseconds(ea.BasicProperties.Timestamp.UnixTime)
                : DateTimeOffset.UtcNow,
            DeliveryAttempt = int.TryParse(headers.GetValueOrDefault("avtobus.attempt"), out var a) ? a : 0,
            TenantId = headers.GetValueOrDefault("x-avb-tenant"),
            TraceParent = headers.GetValueOrDefault("traceparent"),
            PartitionKey = ea.BasicProperties.MessageId,
            Headers = headers.ToFrozenDictionary(),
        };
    }

    private static Guid? ParseGuid(string? s)
        => Guid.TryParse(s, out var g) ? g : null;
}

internal sealed class RabbitAckContext(IChannel channel, ulong deliveryTag) : IAckContext
{
    public ValueTask AckAsync(CancellationToken ct = default)
        => channel.BasicAckAsync(deliveryTag, multiple: false, ct);

    public ValueTask NackAsync(bool requeue = false, CancellationToken ct = default)
        => channel.BasicNackAsync(deliveryTag, multiple: false, requeue, ct);

    public async ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default)
    {
        // Используем TTL-очередь для задержки
        await channel.BasicAckAsync(deliveryTag, multiple: false, ct);
        // В реальном приложении: republish через retry queue с TTL
    }
}

// ── Connection pool ──

internal sealed class ChannelPoolPolicy(IConnection connection) : PooledObjectPolicy<IChannel>
{
    public override IChannel Create() =>
        connection.CreateChannelAsync().GetAwaiter().GetResult();

    public override bool Return(IChannel obj)
    {
        if (obj.IsOpen)
        {
            obj.Dispose();
            return true;
        }
        obj.Dispose();
        return false;
    }
}

// ── Options ──

public sealed class RabbitMqOptions
{
    public string ConnectionString { get; set; } = "";
    public int PrefetchCount { get; set; } = 64;
    public int DeliveryLimit { get; set; } = 6;
    public bool UseQuorumQueues { get; set; } = true;
    public TimeSpan ReconnectDelay { get; set; } = TimeSpan.FromSeconds(5);
}
```
