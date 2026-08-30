using AvtoBus.Runtime;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>
/// Graceful shutdown (идея 35): при остановке in-flight обработки доделываются до конца,
/// а новые сообщения не вычитываются и остаются в очереди.
/// </summary>
public class GracefulShutdownTests
{
    [Fact]
    public async Task Stop_async_drains_in_flight_and_leaves_queued_messages_untouched()
    {
        var gate = new BlockingGate();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus => bus.AddConsumer<BlockingPlaceOrderConsumer>(),
            services => services.AddSingleton(gate));

        // Первое сообщение уходит в обработку и блокируется в хендлере — это in-flight.
        await harness.Bus.SendAsync(new PlaceOrder(Guid.NewGuid(), "in-flight", 10m));
        await gate.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var consumerHost = harness.Services.GetRequiredService<ConsumerHost>();

        // Остановка: приём новых прекращается, in-flight продолжает работать.
        var stop = consumerHost.StopAsync(CancellationToken.None);

        // Дожидаемся, пока все ранеры перестали вычитывать новые сообщения.
        Assert.True(await harness.WaitUntilAsync(
            () => consumerHost.AllReceivingStopped,
            TimeSpan.FromSeconds(5)));

        // Сообщение, отправленное во время дрена, не должно быть вычитано.
        await harness.Bus.SendAsync(new PlaceOrder(Guid.NewGuid(), "queued", 20m));

        // Пока in-flight заблокирован, остановка не завершается.
        Assert.False(stop.IsCompleted);

        // Отпускаем in-flight — только после его завершения StopAsync возвращается.
        gate.Release.TrySetResult();
        await stop.WaitAsync(TimeSpan.FromSeconds(5));

        // In-flight обработан ровно один раз и без сбоев; сообщение дрена осталось в очереди.
        Assert.Equal(1, harness.Recorder.CountConsumed<PlaceOrder>());
        Assert.Empty(harness.Recorder.Faults);
        Assert.Equal(1, harness.Transport.QueueDepths["place-order"]);
    }
}

/// <summary>
/// «Ворота» для теста дрейн-остановки: оба сигнала обёрнуты в один тип, чтобы DI
/// не свалил два параметра одного типа в одну регистрацию.
/// </summary>
public sealed class BlockingGate
{
    public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}

public sealed class BlockingPlaceOrderConsumer(BlockingGate gate) : IConsumer<PlaceOrder>
{
    public async Task ConsumeAsync(ConsumeContext<PlaceOrder> context)
    {
        gate.Started.TrySetResult();
        await gate.Release.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }
}
