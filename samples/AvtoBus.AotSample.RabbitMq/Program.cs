using AvtoBus;
using AvtoBus.RabbitMq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Text.Json.Serialization;

// AOT smoke RabbitMQ-транспорта: round-trip команды через реальный брокер,
// затем выход с кодом 0. Требует RabbitMQ на AVTOBUS_RABBIT_URL (CI: services:).
var rabbitUrl = Environment.GetEnvironmentVariable("AVTOBUS_RABBIT_URL") ?? "amqp://guest:guest@localhost:5672/";

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddAvtoBus(bus =>
{
    bus.UseRabbitMq(opt =>
    {
        opt.ConnectionString = rabbitUrl;
        opt.ClientProvidedName = "avtobus-aot-rabbitmq";
    });
    bus.UseJsonSerializerContext(new AvtoBusJsonContext());
    bus.AddConsumer<OrderHandler>();
    bus.ServiceName("aot-rabbitmq-sample");
});

var app = builder.Build();

await app.StartAsync();

var bus = app.Services.GetRequiredService<IBus>();

var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
OrderHandler.Completed = completed;

await bus.SendAsync(new PlaceOrder("order-1"));

await completed.Task.WaitAsync(TimeSpan.FromSeconds(30));

await app.StopAsync();

Console.WriteLine("RabbitMQ AOT smoke OK");
return 0;

public sealed record PlaceOrder(string OrderId) : ICommand;

[JsonSerializable(typeof(PlaceOrder))]
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
internal sealed partial class AvtoBusJsonContext : JsonSerializerContext { }

public sealed class OrderHandler
{
    public static TaskCompletionSource? Completed { get; set; }

    public static void Handle(PlaceOrder order)
    {
        Console.WriteLine($"Handled {order.OrderId}");
        Completed?.TrySetResult();
    }
}
