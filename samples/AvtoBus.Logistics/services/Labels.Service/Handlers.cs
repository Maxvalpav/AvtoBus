using AvtoBus;
using AvtoBus.Logistics.Contracts.Fulfilment;

namespace Logistics.Labels.Handlers;

public static class LabelHandlers
{
    public static ValueTask Handle(GenerateLabel command, ConsumeContext context)
    {
        var labelUrl = $"https://labels.avtobus.example/{command.ParcelId:N}/label.pdf";
        Console.WriteLine($"[labels] Наклейка для {command.ParcelId:N} → {labelUrl}");

        return context.PublishAsync(new LabelGenerated(command.ParcelId, labelUrl));
    }
}