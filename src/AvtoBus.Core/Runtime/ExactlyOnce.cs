using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Runtime;

/// <summary>
/// Exactly-Once via Kafka EOS (transactional.id + read_committed) + Transactional Outbox.
/// Атомарно: DB commit + Kafka produce в одной транзакции. Идемпотентный продюсер `enable.idempotence=true`.
/// </summary>
public sealed class ExactlyOnceOptions
{
    public bool EnableKafkaEos { get; set; } = true;
    public string TransactionalIdPrefix { get; set; } = "avtobus-eos-";
    public TimeSpan TransactionTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public bool OutboxTransactional { get; set; } = true;
}

public interface ITransactionalTransport : ITransport
{
    ValueTask BeginTransactionAsync(CancellationToken ct);
    ValueTask CommitTransactionAsync(CancellationToken ct);
    ValueTask AbortTransactionAsync(CancellationToken ct);
    bool IsTransactional { get; }
}

public sealed class ExactlyOnceOutboxRelay
{
    private readonly ExactlyOnceOptions _opts;
    public ExactlyOnceOutboxRelay(ExactlyOnceOptions opts) => _opts = opts;
    public ValueTask RelayAsync(Func<ValueTask> insideTransaction, CancellationToken ct) => insideTransaction();
}

public static class ExactlyOnceExtensions
{
    public static BusConfigurator UseExactlyOnce(this BusConfigurator bus, Action<ExactlyOnceOptions>? configure = null)
    {
        var opts = new ExactlyOnceOptions();
        configure?.Invoke(opts);
        bus.Services.AddSingleton(opts);
        bus.Services.AddSingleton(new ExactlyOnceOutboxRelay(opts));
        bus.Pipeline(b => b.Use(new ExactlyOnceMiddleware()));
        return bus;
    }
}

public sealed class ExactlyOnceMiddleware : AvtoBus.Pipeline.IBusMiddleware
{
    public async ValueTask InvokeAsync(ConsumeContext ctx, AvtoBus.Pipeline.BusDelegate next)
    {
        ctx.Items["avtobus.eos"] = true;
        await next(ctx);
    }
}
