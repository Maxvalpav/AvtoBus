using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Dispatch.Handlers;

public static class DispatchHandlers
{
    public static ValueTask Handle(DispatchShipment command, ConsumeContext context)
    {
        Console.WriteLine($"[dispatch] Отправление {command.ShipmentId} передано: ТС {command.VehicleId}, водитель {command.DriverId}");

        return context.PublishAsync(new ShipmentDispatched(command.ShipmentId, command.VehicleId, command.DriverId));
    }
}