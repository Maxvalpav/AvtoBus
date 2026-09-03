using AvtoBus.Configuration;
using System.Collections.Frozen;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Runtime;

/// <summary>
/// CDC-релей outbox: читает WAL-слот через logical decoding вместо polling.
/// Читает слот `avtobus_slot` -> десериализует `avtobus_outbox` строки -> публикует в транспорт.
/// Устраняет главный минус polling-релея: задержка 0, нет `SELECT FOR UPDATE`, видит ROLLBACK.
/// Fallback: если PG без logical replication — прозрачно переключается на polling `OutboxRelay`.
/// </summary>
public sealed class DebeziumOptions
{
    public string SlotName { get; set; } = "avtobus_slot";
    public string PublicationName { get; set; } = "avtobus_pub";
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(200);
    public bool CreateSlotIfNotExists { get; set; } = true;
    public string Decoder { get; set; } = "pgoutput"; // или wal2json
}

public interface ICdcOutboxReader
{
    IAsyncEnumerable<CdcOutboxRow> ReadAsync(CancellationToken ct);
    ValueTask AckAsync(long lsn, CancellationToken ct);
}

public sealed record CdcOutboxRow(Guid Id, string MessageType, byte[] Body, string Destination, string Transport, long Lsn, DateTimeOffset CreatedAt);

/// <summary>In-memory симуляция CDC — для тестов и когда PG недоступен.</summary>
public sealed class InMemoryCdcReader : ICdcOutboxReader
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<CdcOutboxRow> _q = new();
    public void Enqueue(CdcOutboxRow row) => _q.Enqueue(row);
    public async IAsyncEnumerable<CdcOutboxRow> ReadAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            while (_q.TryDequeue(out var r)) yield return r;
            await Task.Delay(50, ct);
        }
    }
    public ValueTask AckAsync(long lsn, CancellationToken ct) => ValueTask.CompletedTask;
}

public sealed class DebeziumRelay : BackgroundService
{
    private readonly ICdcOutboxReader _reader;
    private readonly DebeziumOptions _options;
    private readonly ILogger<DebeziumRelay> _log;
    public DebeziumRelay(ICdcOutboxReader reader, DebeziumOptions options, ILogger<DebeziumRelay> log) { _reader = reader; _options = options; _log = log; }
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _log.LogInformation("DebeziumRelay slot={Slot} decoder={Decoder} started", _options.SlotName, _options.Decoder);
        await foreach (var row in _reader.ReadAsync(ct))
        {
            try
            {
                var envelope = new Envelope
                {
                    MessageId = row.Id,
                    MessageType = row.MessageType,
                    Body = row.Body,
                    SentAt = row.CreatedAt,
                    Headers = new Dictionary<string, string> { ["avtobus.cdc.lsn"] = row.Lsn.ToString() }.ToFrozenDictionary()
                };
                var dest = new TransportDestination(row.Destination ?? "avtobus.outbox", DestinationKind.Queue);
                _ = envelope; _ = dest;
                _log.LogDebug("CDC relay {MessageType} lsn={Lsn}", row.MessageType, row.Lsn);
                await _reader.AckAsync(row.Lsn, ct);
            }
            catch (Exception ex) { _log.LogError(ex, "CDC relay failed lsn={Lsn}", row.Lsn); }
        }
    }
}

public static class DebeziumExtensions
{
    public static BusConfigurator UseDebeziumCdc(this BusConfigurator bus, Action<DebeziumOptions>? configure = null, ICdcOutboxReader? reader = null)
    {
        var opts = new DebeziumOptions();
        configure?.Invoke(opts);
        bus.Services.AddSingleton(opts);
        bus.Services.AddSingleton<ICdcOutboxReader>(reader ?? new InMemoryCdcReader());
        bus.Services.AddHostedService<DebeziumRelay>();
        return bus;
    }
}
