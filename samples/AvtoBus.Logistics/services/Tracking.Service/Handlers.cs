using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Tracking.Handlers;

public static class TrackingHandlers
{
    public static ValueTask Handle(RecordLocation command, ConsumeContext context)
    {
        Console.WriteLine($"[tracking] {command.ShipmentId} @ {command.Latitude:F4},{command.Longitude:F4} в {command.At:HH:mm:ss}");

        return context.PublishAsync(new LocationRecorded(command.ShipmentId, command.Latitude, command.Longitude));
    }
}