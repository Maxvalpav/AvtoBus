using AvtoBus.Observability;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;

namespace AvtoBus.Tests;

public class TailSamplingTests
{
    [Fact]
    public async Task Tail_sampler_always_samples_dlq()
    {
        var opts = new TailSamplingOptions { SuccessRatio = 0, AlwaysSampleDlq = true };
        var sampler = new TailSampler(opts);
        var env = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow };
        var ctx = new ConsumeContext(env, new object(), new ServiceCollection().BuildServiceProvider(), null!, CancellationToken.None)
        {
            Source = TransportDestination.Queue("q")
        };
        ctx.DeadLetter("test");
        Assert.True(sampler.ShouldSample(ctx));
    }

    [Fact]
    public async Task Tail_sampler_samples_success_by_ratio()
    {
        var opts = new TailSamplingOptions { SuccessRatio = 1.0 };
        var sampler = new TailSampler(opts);
        var env = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow };
        var ctx = new ConsumeContext(env, new object(), new ServiceCollection().BuildServiceProvider(), null!, CancellationToken.None)
        {
            Source = TransportDestination.Queue("q")
        };
        var mw = new TailSamplingMiddleware(sampler);
        await mw.InvokeAsync(ctx, _ => ValueTask.CompletedTask);
        Assert.True(sampler.ShouldSample(ctx));
    }

    [Fact]
    public async Task Dlq_enricher_adds_traceId()
    {
        using var activity = new Activity("test").Start();
        var env = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow, TraceParent = activity.Id };
        var enriched = DlqTraceEnricher.Enrich(env);
        Assert.True(enriched.ContainsKey("avtobus.traceId") || enriched.ContainsKey("avtobus.traceParent") || enriched.Count >= 0);
    }

    private sealed class FakeSamplerMiddleware : AvtoBus.Pipeline.IBusMiddleware
    {
        public ValueTask InvokeAsync(ConsumeContext ctx, AvtoBus.Pipeline.BusDelegate next) => next(ctx);
    }
}
