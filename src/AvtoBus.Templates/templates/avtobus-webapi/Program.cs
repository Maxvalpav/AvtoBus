using AvtoBus;
using MyWebApi.Contracts;
#if (transport == "kafka")
using AvtoBus.Kafka;
#elif (transport == "redis")
using AvtoBus.Redis;
#else
using AvtoBus.InMemory;
#endif

var builder = WebApplication.CreateBuilder(args);

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
    bus.ServiceName("my-webapi");
});

var app = builder.Build();

app.MapPost("/orders", async (PlaceOrder command, IBus bus) =>
{
    await bus.SendAsync(command);
    return Results.Accepted();
});

app.Run();

public partial class Program;