using System.Text.Json;
using AvtoBus.AsyncApi;
using AvtoBus.Configuration;
using AvtoBus.EventCatalog;
using AvtoBus.InMemory;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.EventCatalogTests;

public class EventCatalogGeneratorTests
{
    private static (EventCatalogGenerator Generator, ServiceProvider Provider) Build(
        Action<BusConfigurator>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddAvtoBus(bus =>
        {
            bus.UseInMemory();
            bus.AddConsumersFromAssembly(typeof(TestConsumers).Assembly);
            configure?.Invoke(bus);
        });
        services.AddAvtoBusAsyncApi();
        services.AddAvtoBusEventCatalog(opts =>
        {
            opts.Title = "Orders Catalog";
            opts.ServiceName = "orders-api";
        });

        var provider = services.BuildServiceProvider();
        var generator = provider.GetRequiredService<EventCatalogGenerator>();
        return (generator, provider);
    }

    [Fact]
    public void Lists_all_handled_messages()
    {
        var (generator, provider) = Build();
        using var sp = provider;
        var names = generator.Entries.Select(e => e.MessageName).ToArray();
        Assert.Contains("event-catalog-tests.place-order", names);
        Assert.Contains("event-catalog-tests.order-placed", names);
    }

    [Fact]
    public void Marks_command_vs_event()
    {
        var (generator, provider) = Build();
        using var sp = provider;
        var placeOrder = generator.Entries.Single(e => e.MessageName == "event-catalog-tests.place-order");
        var orderPlaced = generator.Entries.Single(e => e.MessageName == "event-catalog-tests.order-placed");

        Assert.True(placeOrder.IsCommand);
        Assert.False(orderPlaced.IsCommand);
    }

    [Fact]
    public void Captures_owners_from_handlers()
    {
        var (generator, provider) = Build();
        using var sp = provider;
        var entry = generator.Entries.Single(e => e.MessageName == "event-catalog-tests.place-order");
        Assert.Contains(entry.Owners, o => o.HandlerName == "TestConsumers.Handle");
    }

    [Fact]
    public void Captures_channel_and_kind()
    {
        var (generator, provider) = Build();
        using var sp = provider;
        var placeOrder = generator.Entries.Single(e => e.MessageName == "event-catalog-tests.place-order");
        var orderPlaced = generator.Entries.Single(e => e.MessageName == "event-catalog-tests.order-placed");

        Assert.Equal("place-order", placeOrder.Channel);
        Assert.Equal("Queue", placeOrder.DestinationKind);
        Assert.Equal("event-catalog-tests.order-placed", orderPlaced.Channel);
        Assert.Equal("Topic", orderPlaced.DestinationKind);
    }

    [Fact]
    public void Schema_json_has_properties()
    {
        var (generator, provider) = Build();
        using var sp = provider;
        var entry = generator.Entries.Single(e => e.MessageName == "event-catalog-tests.place-order");
        using var schema = JsonDocument.Parse(entry.SchemaJson);

        Assert.True(schema.RootElement.GetProperty("properties").TryGetProperty("OrderId", out _));
    }

    [Fact]
    public void Html_contains_title_and_messages()
    {
        var (generator, provider) = Build();
        using var sp = provider;
        var html = generator.GenerateHtml();

        Assert.Contains("Orders Catalog", html);
        Assert.Contains("place-order", html);
        Assert.Contains("order-placed", html);
    }

    [Fact]
    public void Html_escapes_embedded_markup()
    {
        var services = new ServiceCollection();
        services.AddAvtoBus(bus =>
        {
            bus.UseInMemory();
            bus.AddConsumersFromAssembly(typeof(TestConsumers).Assembly);
        });
        services.AddAvtoBusAsyncApi();
        services.AddAvtoBusEventCatalog(opts => opts.Title = "<script>alert(1)</script>");

        var provider = services.BuildServiceProvider();
        using var scope = provider;

        var html = provider.GetRequiredService<EventCatalogGenerator>().GenerateHtml();
        Assert.DoesNotContain("<script>alert", html);
    }

    [Fact]
    public void Json_is_stable_and_has_asyncapi()
    {
        var (generator, provider) = Build();
        using var sp = provider;

        using var doc = JsonDocument.Parse(generator.GenerateJson());
        Assert.True(doc.RootElement.GetProperty("catalog").GetArrayLength() >= 2);
        Assert.Equal("3.0.0", doc.RootElement.GetProperty("asyncapi").GetProperty("asyncapi").GetString());
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
