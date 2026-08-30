using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Drivers.Handlers;

public static class DriverHandlers
{
    public static ValueTask Handle(AssignDriver command, ConsumeContext context)
    {
        Console.WriteLine($"[drivers] Водитель {command.DriverId} назначен на {command.VehicleId}, маршрут {command.RouteId}");

        return context.PublishAsync(new DriverAssigned(command.DriverId, command.VehicleId, command.RouteId));
    }

    public static ValueTask Handle(CompleteShift command, ConsumeContext context)
    {
        Console.WriteLine($"[drivers] Смена водителя {command.DriverId} завершена в {command.EndedAt:HH:mm:ss} (маршрут {command.RouteId})");
        return context.PublishAsync(new ShiftCompleted(command.DriverId, command.RouteId, command.EndedAt));
    }
}