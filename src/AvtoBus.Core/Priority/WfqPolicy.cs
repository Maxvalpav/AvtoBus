using AvtoBus.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Priority;

/// <summary>
/// Weighted Fair Queuing (WFQ) как у SQS + RabbitMQ priority queues.
/// Каждому тенанту/ключу назначается вес; сообщения с большим весом получают буст приоритета.
/// Падает в InMemoryQueue через header `avtobus.wfq-weight`.
/// </summary>
public sealed class WfqOptions
{
    public Dictionary<string, int> TenantWeights { get; } = new();
    public int DefaultWeight { get; set; } = 0;
}

public static class WfqExtensions
{
    public static BusConfigurator UseWfq(this BusConfigurator bus, Action<WfqOptions> configure)
    {
        var opts = new WfqOptions();
        configure(opts);
        bus.Services.AddSingleton(opts);
        bus.Pipeline(b => b.Use(new WfqMiddleware(opts)));
        return bus;
    }
}

public sealed class WfqMiddleware(WfqOptions opts) : AvtoBus.Pipeline.IBusMiddleware
{
    public ValueTask InvokeAsync(ConsumeContext ctx, AvtoBus.Pipeline.BusDelegate next)
    {
        // producer-side weight уже в header через WithPriority; consumer-side — метрика
        if (opts.TenantWeights.TryGetValue(ctx.Envelope.TenantId ?? "", out var w))
            ctx.Items["avtobus.wfq.weight"] = w;
        return next(ctx);
    }
}

public static class PriorityExtensions
{
    public static SendOptions WithWfqWeight(this SendOptions o, int weight) => (SendOptions)o.WithHeader("avtobus.wfq-weight", weight.ToString());
    public static PublishOptions WithWfqWeight(this PublishOptions o, int weight) => (PublishOptions)o.WithHeader("avtobus.wfq-weight", weight.ToString());
}
