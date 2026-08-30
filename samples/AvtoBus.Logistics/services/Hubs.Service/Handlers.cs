using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Hubs.Handlers;

public static class HubHandlers
{
    public static ValueTask Handle(SortThroughHub command, ConsumeContext context)
    {
        var bay = $"B-{command.HubId}-{(command.ShipmentId.GetHashCode() % 20 + 1):D2}";
        Console.WriteLine($"[hubs] {command.ShipmentId} через хаб {command.HubId} → {bay}");

        return context.PublishAsync(new HubProcessed(command.ShipmentId, command.HubId, bay));
    }
}