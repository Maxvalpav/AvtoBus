using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Customs.Handlers;

public static class CustomsHandlers
{
    public static ValueTask Handle(ClearCustoms command, ConsumeContext context)
    {
        var code = $"CL-{command.ShipmentId:N}";
        Console.WriteLine($"[customs] {command.ShipmentId} выпущен по декларации {command.DeclarationNumber}");

        return context.PublishAsync(new CustomsCleared(command.ShipmentId, code));
    }
}