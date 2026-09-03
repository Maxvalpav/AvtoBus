using AvtoBus;
using AvtoBus.InMemory;
using AvtoBus.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AvtoBus.Tests;

/// <summary>
/// Request/response при нескольких транспортах (аудит C2): ответ может прийти
/// не транспортом по умолчанию, и request/response не должен висеть до таймаута.
/// </summary>
public sealed class MultiTransportReplyTests
{
    public sealed record Ping(string Text) : ICommand;

    public sealed record Pong(string Text);

    public static class PingHandlers
    {
        public static Pong Handle(Ping request) => new(request.Text);
    }

    /// <summary>
    /// InMemory с собственным именем: два экземпляра в одном реестре без конфликта имён.
    /// </summary>
    private sealed class NamedTransport(string name, InMemoryTransport inner) : ITransport
    {
        public string Name { get; } = name;

        public ValueTask SendAsync(Envelope envelope, TransportDestination destination, CancellationToken ct = default)
            => inner.SendAsync(envelope, destination, ct);

        public IAsyncEnumerable<ITransportMessage> ReceiveAsync(TransportSubscription subscription, CancellationToken ct = default)
            => inner.ReceiveAsync(subscription, ct);

        public ValueTask ProvisionAsync(IReadOnlyCollection<TransportDestination> destinations, CancellationToken ct = default)
            => inner.ProvisionAsync(destinations, ct);

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    [Fact]
    public async Task Request_response_completes_when_reply_arrives_on_non_default_transport()
    {
        await using var t1 = new InMemoryTransport();
        await using var t2 = new InMemoryTransport();

        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddAvtoBus(bus =>
                {
                    // t1 — транспорт по умолчанию; запрос и ответ идут через t2.
                    bus.UseTransport(new NamedTransport("t1", t1));
                    bus.UseTransport(new NamedTransport("t2", t2));
                    bus.Routes(routes =>
                    {
                        routes.Command<Ping>().ToQueue("pings").Via("t2");
                        routes.Command<Pong>().ToQueue("pongs").Via("t2");
                    });
                    bus.AddConsumer(typeof(PingHandlers));
                });
            })
            .Build();

        await host.StartAsync();
        try
        {
            // Reply-ранер поднят на каждом транспорте, а не только на default.
            var consumerHost = host.Services.GetRequiredService<ConsumerHost>();
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (consumerHost.Runners.Count < 3 && DateTime.UtcNow < deadline)
                await Task.Delay(50);
            Assert.Equal(2, consumerHost.Runners.Count(r => r.Name.StartsWith("reply-", StringComparison.Ordinal)));

            var bus = host.Services.GetRequiredService<IBus>();
            var pong = await bus.RequestAsync<Ping, Pong>(
                new Ping("hello"), TimeSpan.FromSeconds(10));
            Assert.Equal("hello", pong.Text);
        }
        finally
        {
            await host.StopAsync();
        }
    }
}
