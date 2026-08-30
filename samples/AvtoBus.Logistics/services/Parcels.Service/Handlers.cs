using AvtoBus;
using AvtoBus.Logistics.Contracts.Fulfilment;

namespace Logistics.Parcels.Handlers;

public static class ParcelHandlers
{
    public static ValueTask Handle(CreateParcel command, ConsumeContext context)
    {
        var parcelId = $"PRC-{command.OrderId:N}";
        Console.WriteLine($"[parcels] Посылка {parcelId} создана ({command.WeightGrams} г)");

        return context.PublishAsync(new ParcelCreated(command.OrderId, parcelId, command.WeightGrams));
    }
}