using AvtoBus;
using AvtoBus.Logistics.Contracts.Fulfilment;

namespace Logistics.Inventory.Handlers;

public static class InventoryHandlers
{
    // Упрощённая имитация складских остатков: часть SKU может закончиться.
    private static readonly HashSet<Guid> OutOfStock = [Guid.Parse("00000000-0000-0000-0000-00000000000a")];

    public static async ValueTask Handle(ReserveStock command, ConsumeContext context)
    {
        foreach (var line in command.Lines)
        {
            if (OutOfStock.Contains(line.Sku))
            {
                Console.WriteLine($"[inventory] Не хватает SKU {line.Sku}: запрошено {line.Quantity}, есть 0");
                await context.PublishAsync(new StockShortage(command.OrderId, line.Sku, line.Quantity, 0));
                return;
            }
        }

        Console.WriteLine($"[inventory] Резерв по заказу {command.OrderId}: {command.Lines.Count} позиций");
        await context.PublishAsync(new StockReserved(command.OrderId));
    }

    public static async ValueTask Handle(ReleaseStock command, ConsumeContext context)
    {
        foreach (var line in command.Lines)
        {
            Console.WriteLine($"[inventory] Резерв снят по заказу {command.OrderId}: SKU {line.Sku} × {line.Quantity}");
            await context.PublishAsync(new StockReleased(command.OrderId, line.Sku, line.Quantity));
        }
    }
}