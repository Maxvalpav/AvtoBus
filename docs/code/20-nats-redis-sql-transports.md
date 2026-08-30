# AvtoBus.Nats / Redis / SQL — дополнительные транспорты

---

## AvtoBus.Nats/NatsTransport.cs

```csharp
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using System.Text;

namespace AvtoBus.Transport.Nats;

public sealed class NatsTransport : ITransport
{
    public string Name => "nats";

    private readonly NatsConnection _connection;
    private readonly INatsJSContext _js;
    private readonly NatsOptions _options;
    private readonly ILogger<NatsTransport> _log;

    public NatsTransport(NatsOptions options, ILogger<NatsTransport> log)
    {
        _options = options;
        _log = log;
        _connection = new NatsConnection(new NatsOpts { Url = options.Url });
        _js = _connection.CreateJetStreamContext();
    }

    public async ValueTask SendAsync(Envelope envelope, TransportDestination dest, CancellationToken ct = default)
    {
        var headers = new NatsHeaders();
        headers.Add("avtobus-message-type", envelope.MessageType);
        headers.Add("avtobus-message-id", envelope.MessageId.ToString());
        if (envelope.TenantId is not null) headers.Add("avtobus-tenant-id", envelope.TenantId);
        if (envelope.TraceParent is not null) headers.Add("traceparent", envelope.TraceParent);
        foreach (var (k, v) in envelope.Headers) headers.Add($"h-{k}", v);

        var subject = dest.Address.Replace('/', '.');
        var ack = await _js.PublishAsync(subject, envelope.Body.ToArray(), headers: headers, cancellationToken: ct);
        ack.EnsureSuccess();
    }

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription subscription,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        // Ensure stream exists
        foreach (var topic in subscription.Topics)
        {
            try
            {
                await _js.CreateStreamAsync(new StreamConfig(
                    $"avtobus-{topic.Replace('.', '-')}",
                    new[] { $"{topic}.>" }
                )
                {
                    Retention = StreamConfigRetention.Limits,
                    MaxAge = TimeSpan.FromDays(7),
                    Storage = StreamConfigStorage.File,
                }, ct);
            }
            catch { /* stream exists */ }
        }

        // Pull consumer
        var consumerConfig = new ConsumerConfig(subscription.ConsumerGroup)
        {
            DeliverPolicy = ConsumerConfigDeliverPolicy.All,
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = TimeSpan.FromSeconds(30),
            MaxAckPending = subscription.Prefetch,
        };

        var streamName = $"avtobus-{subscription.Topics[0].Replace('.', '-')}";
        var consumer = await _js.CreateOrUpdateConsumerAsync(streamName, consumerConfig, ct);

        await foreach (var msg in consumer.ConsumeAsync<byte[]>(cancellationToken: ct))
        {
            var envelope = ParseEnvelope(msg);
            var ack = new NatsAckContext(msg);
            yield return new TransportMessage(envelope, ack);
        }
    }

    private static Envelope ParseEnvelope(NatsJSMsg<byte[]> msg)
    {
        var headers = new Dictionary<string, string>();
        if (msg.Headers is not null)
            foreach (var (key, values) in msg.Headers)
                headers[key] = values.FirstOrDefault() ?? "";

        return new Envelope
        {
            MessageId = Guid.TryParse(headers.GetValueOrDefault("avtobus-message-id"), out var id) ? id : Guid.NewGuid(),
            MessageType = headers.GetValueOrDefault("avtobus-message-type") ?? "unknown",
            Body = msg.Data ?? [],
            SentAt = DateTimeOffset.UtcNow,
            TenantId = headers.GetValueOrDefault("avtobus-tenant-id"),
            TraceParent = headers.GetValueOrDefault("traceparent"),
            Headers = headers.Where(kv => kv.Key.StartsWith("h-"))
                .ToDictionary(kv => kv.Key[2..], kv => kv.Value).ToFrozenDictionary(),
        };
    }

    public async ValueTask DisposeAsync() => await _connection.DisposeAsync();
    public void Dispose() => _connection.DisposeAsync().AsTask().Wait();
}

internal sealed class NatsAckContext(NatsJSMsg<byte[]> msg) : IAckContext
{
    public ValueTask AckAsync(CancellationToken ct = default) => msg.AckAsync(cancellationToken: ct);
    public async ValueTask NackAsync(bool requeue = false, CancellationToken ct = default)
    {
        if (requeue) await msg.NakAsync(cancellationToken: ct);
        else await msg.AckTerminateAsync(cancellationToken: ct);
    }
    public ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default)
        => msg.NakAsync(delay: delay, cancellationToken: ct);
}

public sealed class NatsOptions { public string Url { get; set; } = "nats://localhost:4222"; }
```

---

## AvtoBus.Redis/RedisStreamTransport.cs

```csharp
using StackExchange.Redis;
using System.Text;

namespace AvtoBus.Transport.Redis;

public sealed class RedisStreamTransport : ITransport
{
    public string Name => "redis";
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<RedisStreamTransport> _log;

    public RedisStreamTransport(string connectionString, ILogger<RedisStreamTransport> log)
    {
        _redis = ConnectionMultiplexer.Connect(connectionString);
        _log = log;
    }

    public async ValueTask SendAsync(Envelope envelope, TransportDestination dest, CancellationToken ct = default)
    {
        var db = _redis.GetDatabase();
        var entries = new NameValueEntry[]
        {
            new("type", envelope.MessageType),
            new("id", envelope.MessageId.ToString()),
            new("body", envelope.Body.ToArray()),
            new("tenant", envelope.TenantId ?? ""),
            new("trace", envelope.TraceParent ?? ""),
            new("sent", envelope.SentAt.ToUnixTimeMilliseconds().ToString()),
        };

        foreach (var (k, v) in envelope.Headers)
            entries = entries.Append(new NameValueEntry($"h:{k}", v)).ToArray();

        await db.StreamAddAsync(dest.Address, entries, maxLength: 1_000_000, useApproximateMaxLength: true);
    }

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription sub,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var db = _redis.GetDatabase();
        var stream = sub.Queue;
        var group = sub.ConsumerGroup;
        var consumer = $"{Environment.MachineName}-{Environment.ProcessId}";

        // Create group if not exists
        try { await db.StreamCreateConsumerGroupAsync(stream, group, "$", createStream: true); }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP")) { }

        // Claim pending first
        var pending = await db.StreamAutoClaimAsync(stream, group, consumer, TimeSpan.FromMinutes(2).Milliseconds, "0-0", count: sub.Prefetch);
        foreach (var entry in pending.ClaimedEntries)
        {
            var envelope = ParseEntry(entry, stream);
            yield return new TransportMessage(envelope, new RedisAck(db, stream, group, entry.Id));
        }

        // Read new
        while (!ct.IsCancellationRequested)
        {
            var results = await db.StreamReadGroupAsync(stream, group, consumer, ">", count: sub.Prefetch);

            if (results.Length == 0)
            {
                await Task.Delay(100, ct);
                continue;
            }

            foreach (var entry in results)
            {
                var envelope = ParseEntry(entry, stream);
                yield return new TransportMessage(envelope, new RedisAck(db, stream, group, entry.Id));
            }
        }
    }

    private static Envelope ParseEntry(StreamEntry entry, string stream)
    {
        var values = entry.Values.ToDictionary(v => (string)v.Name!, v => (string)v.Value!);
        var headers = values.Where(kv => kv.Key.StartsWith("h:"))
            .ToDictionary(kv => kv.Key[2..], kv => kv.Value);

        return new Envelope
        {
            MessageId = Guid.TryParse(values.GetValueOrDefault("id"), out var id) ? id : Guid.NewGuid(),
            MessageType = values.GetValueOrDefault("type") ?? "unknown",
            Body = values.ContainsKey("body") ? Encoding.UTF8.GetBytes(values["body"]) : [],
            SentAt = long.TryParse(values.GetValueOrDefault("sent"), out var ms)
                ? DateTimeOffset.FromUnixTimeMilliseconds(ms) : DateTimeOffset.UtcNow,
            TenantId = values.GetValueOrDefault("tenant") is { Length: > 0 } t ? t : null,
            TraceParent = values.GetValueOrDefault("trace") is { Length: > 0 } tp ? tp : null,
            Headers = headers.ToFrozenDictionary(),
        };
    }

    public ValueTask DisposeAsync() { _redis.Dispose(); return ValueTask.CompletedTask; }
    public void Dispose() => _redis.Dispose();
}

internal sealed class RedisAck(IDatabase db, string stream, string group, RedisValue id) : IAckContext
{
    public async ValueTask AckAsync(CancellationToken ct = default)
        => await db.StreamAcknowledgeAsync(stream, group, id);
    public ValueTask NackAsync(bool requeue = false, CancellationToken ct = default) => ValueTask.CompletedTask;
    public ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default) => ValueTask.CompletedTask;
}
```

---

## AvtoBus.Sql/SqlTransport.cs

```csharp
using Npgsql;

namespace AvtoBus.Transport.Sql;

/// <summary>
/// SQL transport: PostgreSQL таблица-очередь с FOR UPDATE SKIP LOCKED + LISTEN/NOTIFY.
/// Идеален для модульных монолитов.
/// </summary>
public sealed class SqlTransport : ITransport
{
    public string Name => "sql";
    private readonly string _cs;
    private readonly ILogger<SqlTransport> _log;
    private readonly IEnvelopeSerializer _serializer;

    public SqlTransport(string connectionString, IEnvelopeSerializer serializer, ILogger<SqlTransport> log)
    {
        _cs = connectionString;
        _serializer = serializer;
        _log = log;
    }

    public async ValueTask CreateTopologyAsync(TopologyPlan plan, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_cs);
        await conn.OpenAsync(ct);
        foreach (var q in plan.Queues)
        {
            var table = SanitizeName(q.Name);
            await using var cmd = new NpgsqlCommand($"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id BIGSERIAL PRIMARY KEY,
                    envelope BYTEA NOT NULL,
                    visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    claimed_at TIMESTAMPTZ,
                    claimed_by TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS ix_{table}_visible ON {table}(visible_at) WHERE claimed_at IS NULL;
                """, conn);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }

    public async ValueTask SendAsync(Envelope envelope, TransportDestination dest, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_cs);
        await conn.OpenAsync(ct);

        var table = SanitizeName(dest.Address);
        var blob = _serializer.Serialize(envelope);
        var visibleAt = envelope.DeliverAt?.UtcDateTime ?? DateTime.UtcNow;

        await using var cmd = new NpgsqlCommand(
            $"INSERT INTO {table}(envelope, visible_at) VALUES(@e, @v)", conn);
        cmd.Parameters.AddWithValue("e", blob);
        cmd.Parameters.AddWithValue("v", visibleAt);
        await cmd.ExecuteNonQueryAsync(ct);

        // NOTIFY для мгновенного пробуждения консьюмера
        await using var notify = new NpgsqlCommand($"NOTIFY avtobus_{table}", conn);
        await notify.ExecuteNonQueryAsync(ct);
    }

    public async IAsyncEnumerable<TransportMessage> ReceiveAsync(
        TransportSubscription sub,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var table = SanitizeName(sub.Queue);
        var consumer = $"{Environment.MachineName}/{Environment.ProcessId}";

        // LISTEN для push-пробуждения
        await using var listenConn = new NpgsqlConnection(_cs);
        await listenConn.OpenAsync(ct);
        await using var listenCmd = new NpgsqlCommand($"LISTEN avtobus_{table}", listenConn);
        await listenCmd.ExecuteNonQueryAsync(ct);

        while (!ct.IsCancellationRequested)
        {
            await using var conn = new NpgsqlConnection(_cs);
            await conn.OpenAsync(ct);

            // Claim batch: FOR UPDATE SKIP LOCKED
            await using var tx = await conn.BeginTransactionAsync(ct);
            await using var cmd = new NpgsqlCommand($"""
                SELECT id, envelope FROM {table}
                WHERE visible_at <= NOW() AND claimed_at IS NULL
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT {sub.Prefetch}
                """, conn, tx);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var rows = new List<(long Id, byte[] Blob)>();
            while (await reader.ReadAsync(ct))
                rows.Add((reader.GetInt64(0), (byte[])reader.GetValue(1)));
            await reader.CloseAsync();

            if (rows.Count == 0)
            {
                await tx.CommitAsync(ct);
                // Ждём NOTIFY или таймаут
                await listenConn.WaitAsync(ct).WaitAsync(TimeSpan.FromSeconds(5), ct).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                continue;
            }

            foreach (var (id, blob) in rows)
            {
                var envelope = _serializer.Deserialize(blob);
                yield return new TransportMessage(envelope, new SqlAck(conn, table, id, tx));
            }

            await tx.CommitAsync(ct);
        }
    }

    private static string SanitizeName(string name) => name.Replace("-", "_").Replace(".", "_").ToLowerInvariant();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    public void Dispose() { }
}

internal sealed class SqlAck(NpgsqlConnection conn, string table, long id, NpgsqlTransaction tx) : IAckContext
{
    public async ValueTask AckAsync(CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand($"DELETE FROM {table} WHERE id = {id}", conn, tx);
        await cmd.ExecuteNonQueryAsync(ct);
    }
    public async ValueTask NackAsync(bool requeue = false, CancellationToken ct = default)
    {
        if (!requeue)
        {
            await using var cmd = new NpgsqlCommand($"DELETE FROM {table} WHERE id = {id}", conn, tx);
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
    public async ValueTask DeferAsync(TimeSpan delay, CancellationToken ct = default)
    {
        var newVisibleAt = DateTime.UtcNow.Add(delay);
        await using var cmd = new NpgsqlCommand(
            $"UPDATE {table} SET visible_at = '{newVisibleAt:O}', claimed_at = NULL WHERE id = {id}", conn, tx);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```
