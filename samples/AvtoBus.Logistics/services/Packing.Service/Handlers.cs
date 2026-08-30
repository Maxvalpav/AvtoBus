using AvtoBus;
using AvtoBus.Logistics.Contracts.Fulfilment;

namespace Logistics.Packing.Handlers;

public static class PackingHandlers
{
    public static ValueTask Handle(PackOrder command, ConsumeContext context)
    {
        var packageId = $"PKG-{command.OrderId:N}";
        Console.WriteLine($"[packing] Заказ {command.OrderId} упакован на складе {command.WarehouseId}");

        return context.PublishAsync(new OrderPacked(command.OrderId, packageId));
    }
}