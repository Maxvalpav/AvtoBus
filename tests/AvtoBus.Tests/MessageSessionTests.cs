using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>
/// Scoped-сессия транзакционной отправки (ADR-0002): атомарность с бизнес-данными — только
/// через outbox; без outbox сессия доставляет немедленно, как IBus.
/// </summary>
public sealed class MessageSessionTests
{
    [Fact]
    public async Task Handler_with_injected_session_publishes_without_outbox()
    {
        var received = new TaskCompletionSource<OrderPaid>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus
                .AddConsumer(typeof(SessionPublishingHandlers))
                .Subscribe<OrderPaid>((message, _) =>
                {
                    received.TrySetResult(message);
                    return Task.CompletedTask;
                }));

        var orderId = Guid.NewGuid();
        await harness.Bus.SendAsync(new PlaceOrder(orderId, "cust-1", 10m));

        // Хендлер получил IMessageSession параметром из DI и опубликовал через неё.
        var paid = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(orderId, paid.OrderId);
    }

    [Fact]
    public async Task Session_without_outbox_delivers_immediately()
    {
        var received = new TaskCompletionSource<OrderPaid>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.Subscribe<OrderPaid>((message, _) =>
            {
                received.TrySetResult(message);
                return Task.CompletedTask;
            }));

        using var scope = harness.Services.CreateScope();
        var session = scope.ServiceProvider.GetRequiredService<IMessageSession>();
        await session.PublishAsync(new OrderPaid(Guid.NewGuid()));

        // Outbox не подключён: сообщение ушло в транспорт сразу, подписчик получил.
        Assert.NotNull(await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Session_with_outbox_sink_enqueues_instead_of_transport()
    {
        var sink = new RecordingOutboxSink();
        var received = new TaskCompletionSource<OrderPaid>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.Subscribe<OrderPaid>((message, _) =>
            {
                received.TrySetResult(message);
                return Task.CompletedTask;
            }),
            services => services.AddScoped<IOutboxSink>(_ => sink));

        using var scope = harness.Services.CreateScope();
        var session = scope.ServiceProvider.GetRequiredService<IMessageSession>();
        var orderId = Guid.NewGuid();
        await session.PublishAsync(new OrderPaid(orderId));

        // Сообщение записано в outbox, а не ушло в транспорт: подписчик не получил.
        var enqueued = Assert.Single(sink.Enqueued);
        Assert.NotEqual(Guid.Empty, enqueued.Envelope.MessageId);
        var expectedType = harness.Services.GetRequiredService<MessageRegistry>().NameOf(typeof(OrderPaid));
        Assert.Equal(expectedType, enqueued.Envelope.MessageType);
        Assert.False(string.IsNullOrEmpty(enqueued.Destination));
        Assert.False(await WasReceivedAsync(received));
    }

    private static async Task<bool> WasReceivedAsync(TaskCompletionSource<OrderPaid> received)
    {
        // Даём транспорту время, если сообщение (ошибочно) ушло напрямую.
        var completed = await Task.WhenAny(received.Task, Task.Delay(TimeSpan.FromMilliseconds(300)));
        return completed == received.Task;
    }
}

/// <summary>Хендлер, публикующий каскад через внедрённую IMessageSession (а не через возврат).</summary>
public static class SessionPublishingHandlers
{
    public static async Task Handle(PlaceOrder command, IMessageSession session)
    {
        await session.PublishAsync(new OrderPaid(command.OrderId)).ConfigureAwait(false);
    }
}

/// <summary>Фиксирует конверты, отданные сессией в «outbox».</summary>
internal sealed class RecordingOutboxSink : IOutboxSink
{
    public List<(Envelope Envelope, string Destination, string? Transport)> Enqueued { get; } = [];

    public ValueTask EnqueueAsync(Envelope envelope, string destination, string? transport, CancellationToken ct)
    {
        Enqueued.Add((envelope, destination, transport));
        return ValueTask.CompletedTask;
    }
}
