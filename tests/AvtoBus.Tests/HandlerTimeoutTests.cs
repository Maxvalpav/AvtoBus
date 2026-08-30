using AvtoBus.Testing;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>[HandlerTimeout] взводит CancellationToken (идея 170): зависший хендлер прерывается.</summary>
public class HandlerTimeoutTests
{
    [HandlerTimeout("00:00:00.1")]
    private sealed class SlowHandler
    {
        public static TaskCompletionSource<bool> Cancelled = new();

        public async Task Handle(Contracts.TimeoutProbe _message, CancellationToken token)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10), token);
            }
            catch (OperationCanceledException)
            {
                Cancelled.TrySetResult(true);
                throw; // хендлер признаёт отмену — обработка завершается сразу
            }
        }
    }

    [Fact]
    public async Task HandlerTimeout_cancels_in_flight_handler()
    {
        SlowHandler.Cancelled = new();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer(typeof(SlowHandler)));

        await harness.Bus.PublishAsync(new Contracts.TimeoutProbe(Guid.NewGuid()));

        Assert.True(await SlowHandler.Cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5)),
            "[HandlerTimeout] did not cancel the in-flight handler");
    }

    [Fact]
    public async Task Handler_without_timeout_is_not_cancelled()
    {
        var handled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var harness = await AvtoBusTestHarness.StartAsync(bus =>
            bus.Subscribe<Contracts.OrderPlaced>((_, _) =>
            {
                handled.TrySetResult(true);
                return Task.CompletedTask;
            }));

        await harness.Bus.PublishAsync(new Contracts.OrderPlaced(Guid.NewGuid(), 10m));

        Assert.True(await handled.Task.WaitAsync(TimeSpan.FromSeconds(5)),
            "handler did not run");
    }
}
