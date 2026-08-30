using AvtoBus;
using AvtoBus.Logistics.Contracts.Fulfilment;

namespace Logistics.Warehouses.Handlers;

public static class WarehouseHandlers
{
    public static ValueTask Handle(AllocateWarehouse command, ConsumeContext context)
    {
        var warehouseId = $"WH-{command.Region.ToUpperInvariant()}-01";
        Console.WriteLine($"[warehouses] Заказ {command.OrderId} выделен на склад {warehouseId}");

        return context.PublishAsync(new WarehouseAllocated(command.OrderId, warehouseId));
    }
}