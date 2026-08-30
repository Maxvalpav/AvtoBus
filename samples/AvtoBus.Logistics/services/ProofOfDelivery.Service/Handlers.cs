using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.ProofOfDelivery.Handlers;

public static class PodHandlers
{
    public static ValueTask Handle(CapturePod command, ConsumeContext context)
    {
        Console.WriteLine($"[pod] Подтверждение по {command.ShipmentId}: подпись «{command.RecipientSignature}» в {command.At:HH:mm:ss}");

        return context.PublishAsync(new PodCaptured(command.ShipmentId, command.RecipientSignature));
    }
}