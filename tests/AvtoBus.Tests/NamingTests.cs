using AvtoBus.Tests.Contracts;

namespace AvtoBus.Tests;

public class NamingTests
{
    [Theory]
    [InlineData("OrderPlaced", "order-placed")]
    [InlineData("PlaceOrder", "place-order")]
    [InlineData("A", "a")]
    [InlineData("HTTPServer", "http-server")]
    [InlineData("OrderID", "order-id")]
    [InlineData("already-kebab", "already-kebab")]
    public void KebabCase_splits_on_word_boundaries(string input, string expected)
        => Assert.Equal(expected, MessageTypeNaming.ToKebabCase(input));

    [Fact]
    public void Conventional_name_combines_namespace_segment_and_type()
        => Assert.Equal("contracts.order-placed", MessageTypeNaming.NameOf<OrderPlaced>());

    [Fact]
    public void MessageAlias_overrides_convention()
        => Assert.Equal("orders.legacy-renamed.v2", MessageTypeNaming.NameOf<RenamedContract>());

    [Fact]
    public void Legacy_aliases_are_kept_for_receiving()
    {
        var aliases = MessageTypeNaming.AliasesOf(typeof(RenamedContract));

        Assert.Contains("orders.legacy-renamed.v2", aliases);
        Assert.Contains("orders.legacy-renamed.v1", aliases);
    }

    [Fact]
    public void Registry_resolves_both_canonical_and_legacy_names()
    {
        var registry = MessageRegistry.Build([typeof(RenamedContract)]);

        Assert.True(registry.TryResolve("orders.legacy-renamed.v2", out var canonical));
        Assert.True(registry.TryResolve("orders.legacy-renamed.v1", out var legacy));
        Assert.Equal(typeof(RenamedContract), canonical);
        Assert.Equal(typeof(RenamedContract), legacy);
    }

    [Fact]
    public void Registry_rejects_two_types_claiming_one_name()
    {
        // Оба типа претендуют на одно и то же конвенционное имя — это ошибка конфигурации,
        // и поймать её нужно при старте, а не когда сообщение уйдёт не тому хендлеру.
        var exception = Assert.Throws<InvalidOperationException>(
            () => MessageRegistry.Build([typeof(DuplicateOrderPlaced), typeof(OrderPlaced)]));

        Assert.Contains("занято типами", exception.Message);
    }

    [Fact]
    public void Command_queue_name_drops_namespace_prefix()
        => Assert.Equal("place-order", Configuration.RoutingTable.CommandQueueName(typeof(PlaceOrder)));
}

/// <summary>
/// Тип, намеренно претендующий на чужое имя контракта — нужен для проверки,
/// что реестр ловит коллизию при старте.
/// </summary>
[MessageAlias("contracts.order-placed")]
public sealed record DuplicateOrderPlaced(Guid OrderId);
