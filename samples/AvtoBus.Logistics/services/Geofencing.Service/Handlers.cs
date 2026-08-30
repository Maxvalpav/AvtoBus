using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Geofencing.Handlers;

public static class GeofencingHandlers
{
    public static ValueTask Handle(GeofenceEntered command, ConsumeContext context)
    {
        Console.WriteLine($"[geofencing] {command.ShipmentId} вошло в зону {command.ZoneId}");

        return context.PublishAsync(new GeofenceCrossed(command.ShipmentId, command.ZoneId, "enter"));
    }
}