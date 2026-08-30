using AvtoBus;
using AvtoBus.InMemory;
using AvtoBus.Priority;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AvtoBus.Tests;

public class PriorityQueueTests
{
    [Fact]
    public async Task Priority_queue_delivers_high_priority_first()
    {
        var time = new FakeTimeProvider();
        var transport = new InMemoryTransport(time, 10);
        var dest = TransportDestination.Queue("test");
        var low = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = time.GetUtcNow(), Priority = 0 };
        var high = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = time.GetUtcNow(), Priority = 10 };
        var mid = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = time.GetUtcNow(), Priority = 5 };

        await transport.SendAsync(low, dest, CancellationToken.None);
        await transport.SendAsync(high, dest, CancellationToken.None);
        await transport.SendAsync(mid, dest, CancellationToken.None);

        var sub = new TransportSubscription(dest, "g");
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var received = new List<int>();
        await foreach (var msg in transport.ReceiveAsync(sub, cts.Token))
        {
            received.Add(msg.Envelope.Priority);
            await msg.AcknowledgeAsync(CancellationToken.None);
            if (received.Count == 3) break;
        }
        await transport.DisposeAsync();
        Assert.Equal([10, 5, 0], received);
    }

    [Fact]
    public async Task InMemory_transport_respects_priority_header()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAvtoBus(bus =>
        {
            bus.ServiceName("test");
            bus.UseTransport(new InMemoryTransport());
        });
        using var sp = services.BuildServiceProvider();
        var bus = sp.GetRequiredService<IBus>();
        var host = sp.GetRequiredService<IHostedService>();
        await host.StartAsync(CancellationToken.None);

        var lowTcs = new TaskCompletionSource<int>();
        var highTcs = new TaskCompletionSource<int>();
        var order = new List<string>();
        // Register handlers via bus configurator not possible here — test queue directly via transport API
        var transport = sp.GetServices<ITransport>().OfType<InMemoryTransport>().Single();
        await transport.SendAsync(new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow, Priority = 0 }, TransportDestination.Queue("q"), CancellationToken.None);
        await transport.SendAsync(new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow, Priority = 10 }, TransportDestination.Queue("q"), CancellationToken.None);

        var sub = new TransportSubscription(TransportDestination.Queue("q"), "g");
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var first = await ReadFirstPriority(transport, sub, cts.Token);
        Assert.Equal(10, first);

        await host.StopAsync(CancellationToken.None);
    }

    private static async Task<int> ReadFirstPriority(InMemoryTransport t, TransportSubscription sub, CancellationToken ct)
    {
        await foreach (var m in t.ReceiveAsync(sub, ct))
        {
            return m.Envelope.Priority;
        }
        return -1;
    }

    [Fact]
    public void WithPriority_sets_header_and_value()
    {
        var opts = new SendOptions().WithPriority(7);
        Assert.Equal(7, opts.Priority);
        Assert.Equal("7", opts.Headers["avtobus.priority"]);
    }

    [Fact]
    public void Wfq_extension_sets_weight_header()
    {
        var opts = new SendOptions().WithWfqWeight(5);
        Assert.Equal("5", opts.Headers["avtobus.wfq-weight"]);
    }
}
