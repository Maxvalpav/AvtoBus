using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Fleet.Handlers;

public static class FleetHandlers
{
    public static ValueTask Handle(RegisterVehicle command, ConsumeContext context)
    {
        Console.WriteLine($"[fleet] ТС {command.RegistrationPlate} ({command.Type}), {command.CapacityKg} кг");

        return context.PublishAsync(new VehicleRegistered(command.VehicleId, command.RegistrationPlate, command.Type));
    }

    public static ValueTask Handle(SetVehicleOutOfService command, ConsumeContext context)
    {
        Console.WriteLine($"[fleet] ТС {command.VehicleId} выведено из эксплуатации: «{command.Reason}»");
        return context.PublishAsync(new VehicleOutOfService(command.VehicleId, command.Reason));
    }
}