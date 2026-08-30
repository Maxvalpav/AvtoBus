using AvtoBus.InMemory;
using AvtoBus.Runtime;

namespace AvtoBus.Tests;

/// <summary>
/// Back-pressure in-memory транспорта (идея 353): bounded-канал не даёт флуду
/// съесть память без границ — переполнение превращается в блокировку паблишера.
/// </summary>
public sealed class BackpressureTests
{
    [Fact]
    public async Task Flood_blocks_at_capacity_instead_of_growing_unbounded()
    {
        const int capacity = 2;
        var transport = new InMemoryTransport(TimeProvider.System, capacity);

        var delivered = 0;
        const string queueName = "orders::workers";
        var subscription = new TransportSubscription(TransportDestination.Queue(queueName), "workers");

        // Медленный консьюмер: вычитывает по одному сообщению каждые 30 мс.
        var consumer = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in transport.ReceiveAsync(subscription))
                {
                    Interlocked.Increment(ref delivered);
                    await Task.Delay(30);
                }
            }
            catch (OperationCanceledException)
            {
                // Штатная остановка транспорта.
            }
        });

        try
        {
            // Флуд: 50 паблишеров в очередь с bounded-каналом на 2 сообщения.
            var publishes = Enumerable.Range(0, 50)
                .Select(_ => transport.SendAsync(NewEnvelope(), TransportDestination.Queue(queueName)).AsTask())
                .ToArray();

            // Флуд действительно упирается в границу: буфер насыщается.
            Assert.True(await WaitUntilAsync(
                () => transport.QueueDepths.GetValueOrDefault(queueName) >= capacity,
                TimeSpan.FromSeconds(5)),
                "Очередь так и не наполнилась до capacity — back-pressure не задействован.");

            // Память ограничена: глубина очереди никогда не превышает capacity,
            // переполнение блокирует паблишера (FullMode.Wait), а не растёт в памяти.
            for (var i = 0; i < 20; i++)
            {
                Assert.True(transport.QueueDepths.GetValueOrDefault(queueName) <= capacity);
                await Task.Delay(25);
            }

            // Заблокированные паблишеры разблокируются по мере вычитки: ничего не потеряно.
            await Task.WhenAll(publishes);

            Assert.True(await WaitUntilAsync(() => Volatile.Read(ref delivered) >= 50, TimeSpan.FromSeconds(10)));
        }
        finally
        {
            await transport.DisposeAsync();
            await consumer;
        }
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;
            await Task.Delay(10);
        }
        return condition();
    }

    private static Envelope NewEnvelope()
        => new()
        {
            MessageId = Guid.NewGuid(),
            MessageType = "contracts.place-order",
            Body = """{"total":42}"""u8.ToArray(),
            ContentType = "application/json",
            SentAt = DateTimeOffset.UtcNow,
        };
}
