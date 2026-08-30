using System.Text.Json;
using AvtoBus.AsyncApi;
using AvtoBus.Configuration;
using AvtoBus.Handlers;
using AvtoBus.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace AvtoBus.AsyncApiTests;

public class AsyncApiGeneratorTests
{
    private const string QueueName = "place-order";

    private static (AsyncApiGenerator Generator, ServiceProvider Provider) Build(
        Action<BusConfigurator> configure)
    {
        var services = new ServiceCollection();
        services.AddAvtoBus(bus =>
        {
            bus.UseInMemory();
            configure(bus);
        });
        services.AddAvtoBusAsyncApi(info => info.Servers["local"] = new { url = "memory://local", protocol = "amqp" });

        var provider = services.BuildServiceProvider();
        var generator = provider.GetRequiredService<AsyncApiGenerator>();
        return (generator, provider);
    }

    private static JsonDocument Generate()
    {
        var (generator, provider) = Build(bus =>
        {
            bus.AddConsumersFromAssembly(typeof(TestConsumers).Assembly);
        });

        using var scope = provider;
        return JsonDocument.Parse(generator.Generate());
    }

    [Fact]
    public void Produces_asyncapi_30_document()
    {
        using var doc = Generate();

        Assert.Equal("3.0.0", doc.RootElement.GetProperty("asyncapi").GetString());
    }

    [Fact]
    public void Emits_channel_for_command_queue()
    {
        using var doc = Generate();

        Assert.True(doc.RootElement.GetProperty("channels").TryGetProperty(QueueName, out _));
    }

    [Fact]
    public void Emits_channel_for_event_topic()
    {
        using var doc = Generate();

        var channels = doc.RootElement.GetProperty("channels");
        var names = channels.EnumerateObject().Select(p => p.Name).ToArray();
        Assert.Contains("async-api-tests.order-placed", names);
    }

    [Fact]
    public void Emits_receive_operation()
    {
        using var doc = Generate();

        var ops = doc.RootElement.GetProperty("operations");
        var found = ops.EnumerateObject().Any(p =>
            p.Value.GetProperty("action").GetString() == "receive" &&
            p.Value.GetProperty("channel").GetProperty("$ref").GetString() == $"#/channels/{QueueName}");

        Assert.True(found);
    }

    [Fact]
    public void Emits_schema_with_pascal_case_properties()
    {
        using var doc = Generate();

        var schemas = doc.RootElement.GetProperty("components").GetProperty("schemas");
        Assert.True(schemas.TryGetProperty("PlaceOrder", out var schema));
        Assert.True(schema.GetProperty("properties").TryGetProperty("OrderId", out _));
    }

    [Fact]
    public void Serializes_servers_from_info()
    {
        using var doc = Generate();

        var servers = doc.RootElement.GetProperty("servers");
        Assert.True(servers.TryGetProperty("local", out var server));
        Assert.Equal("memory://local", server.GetProperty("url").GetString());
    }

    [Fact]
    public void Resolves_explicit_route_override()
    {
        var (generator, provider) = Build(bus =>
        {
            bus.AddConsumersFromAssembly(typeof(TestConsumers).Assembly);
            bus.Routes(cfg => cfg.Command<PlaceOrder>().ToQueue("orders.v1"));
        });
        using var scope = provider;

        using var doc = JsonDocument.Parse(generator.Generate());
        Assert.True(doc.RootElement.GetProperty("channels").TryGetProperty("orders.v1", out _));
    }
}

public class PlaceOrder : ICommand
{
    public required string OrderId { get; init; }
    public int Amount { get; init; }
}

public class OrderPlaced : IEvent
{
    public required string OrderId { get; init; }
}

public static class TestConsumers
{
    public static void Handle(PlaceOrder command) { }
    public static void Consume(OrderPlaced @event) { }
}
