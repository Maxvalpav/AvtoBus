using AvtoBus;
using AvtoBus.Logistics.Contracts.Finance;

namespace Logistics.Insurance.Handlers;

public static class InsuranceHandlers
{
    public static ValueTask Handle(InsureShipment command, ConsumeContext context)
    {
        var policy = $"POL-{command.ShipmentId:N}";
        Console.WriteLine($"[insurance] {command.ShipmentId} застрахован на {command.InsuredValue:C} ({policy})");

        return context.PublishAsync(new ShipmentInsured(command.ShipmentId, policy, command.InsuredValue));
    }
}