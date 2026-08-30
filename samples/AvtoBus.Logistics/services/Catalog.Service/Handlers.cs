using AvtoBus;
using AvtoBus.Logistics.Contracts.Ordering;

namespace Logistics.Catalog.Handlers;

public static class CatalogHandlers
{
    public static ValueTask Handle(CreateProduct command, ConsumeContext context)
    {
        Console.WriteLine($"[catalog] Товар {command.Sku}: {command.Name} ({command.Category}), {command.UnitPrice:C}");

        return context.PublishAsync(new ProductCreated(command.Sku, command.Name, command.Category, command.UnitPrice));
    }
}