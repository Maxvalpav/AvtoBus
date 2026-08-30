using AvtoBus.Configuration;
using AvtoBus.Runtime;
using AvtoBus.Sql;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AvtoBus.Tests;

/// <summary>
/// Отложенный ретрай должен быть durable: сообщение с будущим DeliverAt живёт в брокере
/// (для SQL — строка с visible_at), а не в памяти консьюмера. Рестарт процесса в момент
/// задержки не должен терять ретрай — его подхватывает новый инстанс. (строка матрицы
/// «Delayed retry durable»; идеи 164, 66).
/// </summary>
public sealed class DelayedRetryDurabilityTests
{
    private const string EnvUrl = "AVTOBUS_PG_URL";

    [Fact]
    public async Task Delayed_retry_survives_host_restart_during_backoff()
    {
        var cs = await PostgresTestHost.CreateDatabaseAsync();
        if (cs is null)
            Assert.Skip("PostgreSQL недоступен: задайте AVTOBUS_PG_URL");

        // Уникальный префикс: рестарт того же узла читает те же таблицы.
        var tablePrefix = $"avtobus_{Guid.NewGuid():N}_";

        var hostAAttempts = 0;
        var hostBAttempts = 0;

        Action<BusConfigurator> configure = bus => bus
            .Recoverability(r => r
                .ImmediateRetries(0)
                .DelayedRetries(1, Backoff.Fixed(TimeSpan.FromSeconds(5))))
            .Subscribe<PlaceOrder>((_, _) =>
            {
                Interlocked.Increment(ref hostAAttempts);
                throw new InvalidOperationException("первая попытка всегда падает");
            });

        // Host A: поднимаем, публикуем, ждём первую (упавшую) попытку.
        var hostA = await StartBusHostAsync(cs, tablePrefix, configure);
        var busA = hostA.Services.GetRequiredService<IBus>();
        try
        {
            await busA.SendAsync(new PlaceOrder(Guid.NewGuid(), "cust-1", 100));
            Assert.True(
                await WaitUntilAsync(() => Volatile.Read(ref hostAAttempts) >= 1, TimeSpan.FromSeconds(10)),
                "Host A не обработал первую попытку.");
        }
        finally
        {
            // Ретрай запланирован с DeliverAt = now + 5s. Рестартуем ДО наступления задержки:
            // вторая попытка обязана быть доставлена новым инстансом — это и есть durability.
            await hostA.StopAsync(CancellationToken.None);
            hostA.Dispose();
        }

        // Host B: тот же брокер (та же БД, тот же префикс), но счётчик попыток свой.
        var hostB = await StartBusHostAsync(cs, tablePrefix, bus => bus
            .Recoverability(r => r
                .ImmediateRetries(0)
                .DelayedRetries(1, Backoff.Fixed(TimeSpan.FromSeconds(5))))
            .Subscribe<PlaceOrder>((_, _) =>
            {
                Interlocked.Increment(ref hostBAttempts);
                throw new InvalidOperationException("вторая попытка тоже падает");
            }));
        try
        {
            Assert.True(
                await WaitUntilAsync(() => Volatile.Read(ref hostBAttempts) >= 1, TimeSpan.FromSeconds(20)),
                "Отложенный ретрай не пережил рестарт: host B не получил вторую попытку.");

            // Host A ни при каких обстоятельствах не должен увидеть вторую попытку.
            Assert.Equal(1, Volatile.Read(ref hostAAttempts));
        }
        finally
        {
            await hostB.StopAsync(CancellationToken.None);
            hostB.Dispose();
        }
    }

    [Fact]
    public async Task Delayed_retry_fires_in_memory_bus()
    {
        // Контрольный тест: отложенный ретрай срабатывает в живой шине (attempt 1 падает,
        // attempt 2 приходит после бэкоффа). In-memory транспорт не durable — рестарт теряет
        // ретрай, поэтому durability требует брокера (см. тест выше на PostgreSQL).
        var attempts = 0;

        await using var harness = await AvtoBus.Testing.AvtoBusTestHarness.StartAsync(bus => bus
            .Recoverability(r => r
                .ImmediateRetries(0)
                .DelayedRetries(1, Backoff.Fixed(TimeSpan.FromMilliseconds(100))))
            .Subscribe<OrderPaid>((_, _) =>
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("всегда падает");
            }));

        await harness.Bus.PublishAsync(new OrderPaid(Guid.NewGuid()));

        Assert.True(
            await harness.WaitUntilAsync(() => Volatile.Read(ref attempts) >= 1, TimeSpan.FromSeconds(10)),
            "Первая попытка не обработана.");

        Assert.True(
            await harness.WaitUntilAsync(() => Volatile.Read(ref attempts) >= 2, TimeSpan.FromSeconds(10)),
            "Отложенный ретрай не сработал в in-memory шине.");
    }

    private static async Task<IHost> StartBusHostAsync(
        string connectionString,
        string tablePrefix,
        Action<BusConfigurator> configure)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        builder.Services.AddAvtoBus(bus =>
        {
            bus.UseSql(sql =>
            {
                sql.ConnectionString = connectionString;
                sql.TablePrefix = tablePrefix;
                sql.ReclaimTimeout = TimeSpan.FromSeconds(30);
                sql.ListenTimeout = TimeSpan.FromMilliseconds(200);
            });

            configure(bus);
        });

        var host = builder.Build();
        await host.StartAsync();

        // Даём консьюмерам подписаться, иначе первое сообщение уйдёт в пустоту.
        await WaitForConsumersAsync(host);
        return host;
    }

    private static async Task WaitForConsumersAsync(IHost host)
    {
        var consumerHost = host.Services.GetRequiredService<ConsumerHost>();
        for (var i = 0; i < 200 && consumerHost.Runners.Count == 0; i++)
            await Task.Delay(5);

        await Task.Delay(50);
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return true;
            await Task.Delay(25);
        }

        return condition();
    }
}
