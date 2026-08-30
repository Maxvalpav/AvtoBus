using AvtoBus;
using MyWorker.Contracts;
#if (transport == "kafka")
using AvtoBus.Kafka;
#elif (transport == "redis")
using AvtoBus.Redis;
#else
using AvtoBus.InMemory;
#endif

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddAvtoBus(bus =>
{
#if (transport == "kafka")
    bus.UseKafka(k => k.BootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092");
#elif (transport == "redis")
    bus.UseRedis(r => r.ConnectionString = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379");
#else
    bus.UseInMemory();
#endif

    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
    bus.ServiceName(typeof(Program).Assembly.GetName().Name ?? "worker");
});

var app = builder.Build();

// Таймер для примера: шлём команду раз в 5 секунд, консьюмер её обработает.
using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
var bus = app.Services.GetRequiredService<IBus>();
_ = Task.Run(async () =>
{
    var n = 0;
    while (await timer.WaitForNextTickAsync())
    {
        await bus.SendAsync(new PlaceOrder { OrderId = $"order-{++n}" });
    }
});

await app.RunAsync();
