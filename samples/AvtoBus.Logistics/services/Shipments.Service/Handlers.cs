using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Shipments.Handlers;

public static class ShipmentHandlers
{
    public static ValueTask Handle(CreateShipment command, ConsumeContext context)
    {
        Console.WriteLine($"[shipments] Отправление {command.ShipmentId} создано: {command.Origin} → {command.Destination}");

        return context.PublishAsync(new ShipmentCreated(command.ShipmentId, command.OrderId, command.ParcelId));
    }
}