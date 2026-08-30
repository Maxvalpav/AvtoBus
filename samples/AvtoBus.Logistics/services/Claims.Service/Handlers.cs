using AvtoBus;
using AvtoBus.Logistics.Contracts.Finance;

namespace Logistics.Claims.Handlers;

public static class ClaimHandlers
{
    public static ValueTask Handle(FileClaim command, ConsumeContext context)
    {
        var number = $"CLM-{command.ClaimId:N}";
        Console.WriteLine($"[claims] Претензия {number} по {command.ShipmentId}: «{command.Reason}», {command.Amount:C}");

        return context.PublishAsync(new ClaimFiled(command.ClaimId, command.ShipmentId, number));
    }
}