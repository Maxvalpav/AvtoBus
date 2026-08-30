using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Scheduling.Handlers;

public static class SchedulingHandlers
{
    public static ValueTask Handle(SchedulePickup command, ConsumeContext context)
    {
        var slot = $"SLOT-{command.ShipmentId:N}";
        Console.WriteLine($"[scheduling] Забор {command.ShipmentId} из {command.Origin} запланирован {command.WindowStart:dd.MM HH:mm}—{command.WindowEnd:HH:mm}");

        return context.PublishAsync(new PickupScheduled(command.ShipmentId, slot));
    }
}