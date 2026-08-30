using AvtoBus.Runtime;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

public class RequestStreamingTests
{
    [Fact]
    public async Task Streaming_router_push_and_read_stream()
    {
        var router = new StreamingReplyRouter();
        var id = Guid.NewGuid();
        var stream = router.RegisterStream<string>(id, TimeSpan.FromSeconds(2), CancellationToken.None);
        router.TryPush(id, "hello");
        router.TryPush(id, "world");
        router.TryComplete(id);

        var list = new List<string>();
        await foreach (var item in stream) list.Add(item);
        Assert.Equal(["hello", "world"], list);
    }

    [Fact]
    public async Task Stream_consume_context_enqueues_chunks()
    {
        var env = new Envelope { MessageId = Guid.NewGuid(), MessageType = "req", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow };
        var ctx = new ConsumeContext(env, new object(), new ServiceCollection().BuildServiceProvider(), null!, CancellationToken.None)
        {
            Source = TransportDestination.Queue("q")
        };
        await ctx.StreamAsync("chunk1");
        await ctx.StreamAsync("chunk2");
        ctx.CompleteStream();
        Assert.Equal(3, ctx.Outgoing.Count);
        Assert.Equal("chunk", ctx.Outgoing[0].Options?.Headers["avtobus.stream"] ?? "chunk");
    }

    [Fact]
    public async Task Streaming_router_fail_propagates_exception()
    {
        var router = new StreamingReplyRouter();
        var id = Guid.NewGuid();
        var stream = router.RegisterStream<string>(id, TimeSpan.FromSeconds(2), CancellationToken.None);
        router.TryFail(id, new InvalidOperationException("boom"));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in stream) { }
        });
    }
}
