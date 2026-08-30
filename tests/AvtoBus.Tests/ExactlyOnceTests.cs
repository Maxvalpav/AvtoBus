using AvtoBus.Configuration;
using AvtoBus.Runtime;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

public class ExactlyOnceTests
{
    [Fact]
    public void UseExactlyOnce_registers_options_and_middleware()
    {
        var services = new ServiceCollection();
        services.AddAvtoBus(bus => bus.UseExactlyOnce(o => o.TransactionalIdPrefix = "test-"));
        using var sp = services.BuildServiceProvider();
        var opts = sp.GetRequiredService<ExactlyOnceOptions>();
        Assert.Equal("test-", opts.TransactionalIdPrefix);
        Assert.True(opts.EnableKafkaEos);
    }

    [Fact]
    public async Task ExactlyOnce_middleware_marks_context()
    {
        var mw = new ExactlyOnceMiddleware();
        var env = new Envelope { MessageId = Guid.NewGuid(), MessageType = "a", Body = ReadOnlyMemory<byte>.Empty, SentAt = DateTimeOffset.UtcNow };
        var ctx = new ConsumeContext(env, new object(), new ServiceCollection().BuildServiceProvider(), null!, CancellationToken.None)
        {
            Source = TransportDestination.Queue("q")
        };
        await mw.InvokeAsync(ctx, _ => ValueTask.CompletedTask);
        Assert.Equal(true, ctx.Items["avtobus.eos"]);
    }

    [Fact]
    public async Task Transactional_transport_interface_can_begin_commit()
    {
        var fake = new FakeTransactionalTransport();
        await fake.BeginTransactionAsync(CancellationToken.None);
        Assert.True(fake.IsTransactional);
        await fake.CommitTransactionAsync(CancellationToken.None);
        Assert.Equal(1, fake.Commits);
    }

    private sealed class FakeTransactionalTransport : ITransactionalTransport
    {
        public string Name => "fake";
        public bool IsTransactional => true;
        public int Commits { get; private set; }
        public ValueTask BeginTransactionAsync(CancellationToken ct) => ValueTask.CompletedTask;
        public ValueTask CommitTransactionAsync(CancellationToken ct) { Commits++; return ValueTask.CompletedTask; }
        public ValueTask AbortTransactionAsync(CancellationToken ct) => ValueTask.CompletedTask;
        public ValueTask SendAsync(Envelope e, TransportDestination d, CancellationToken ct = default) => ValueTask.CompletedTask;
        public IAsyncEnumerable<ITransportMessage> ReceiveAsync(TransportSubscription s, CancellationToken ct = default) => AsyncEnumerable.Empty<ITransportMessage>();
        public ValueTask ProvisionAsync(IReadOnlyCollection<TransportDestination> d, CancellationToken ct = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
