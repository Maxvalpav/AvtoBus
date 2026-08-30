using AvtoBus;
using AvtoBus.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Text.Json.Serialization;

// AOT smoke: публикация команды и её обработка на InMemory, затем выход с кодом 0.
// AOT-путь: хендлер покрыт генератором (сгенерированный диспетчер, без рефлексии),
// а сериализация идёт через source-generated JsonSerializerContext.
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddAvtoBus(bus =>
{
    bus.UseInMemory();
    bus.UseJsonSerializerContext(new AvtoBusJsonContext());
    bus.AddConsumer<OrderHandler>();
    bus.ServiceName("aot-sample");
});

var app = builder.Build();

await app.StartAsync();

var bus = app.Services.GetRequiredService<IBus>();

var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
OrderHandler.Completed = completed;

await bus.SendAsync(new PlaceOrder("order-1"));

// Ждём, пока обработчик реально отработает через транспорт.
await completed.Task.WaitAsync(TimeSpan.FromSeconds(10));

await app.StopAsync();

Console.WriteLine("AOT smoke OK");
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