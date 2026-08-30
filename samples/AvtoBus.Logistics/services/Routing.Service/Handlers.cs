using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Routing.Handlers;

public static class RoutingHandlers
{
    public static ValueTask Handle(PlanRoute command, ConsumeContext context)
    {
        var routeId = $"RT-{command.ShipmentId:N}";
        var minutes = 30 + command.Waypoints.Count * 10;
        Console.WriteLine($"[routing] Маршрут {routeId} для {command.ShipmentId}: {command.Origin} → {command.Destination}, {minutes} мин");

        return context.PublishAsync(new RoutePlanned(command.ShipmentId, routeId, minutes));
    }

    public static ValueTask Handle(RerouteShipment command, ConsumeContext context)
    {
        var routeId = $"RT-{command.ShipmentId:N}";
        var minutes = 30 + command.NewWaypoints.Count * 10;
        Console.WriteLine($"[routing] Маршрут {routeId} для {command.ShipmentId} перестроен: «{command.Reason}», {minutes} мин");

        return context.PublishAsync(new ShipmentRerouted(command.ShipmentId, routeId, minutes, command.Reason));
    }
}