using AvtoBus;
using AvtoBus.Logistics.Contracts.Ordering;

namespace Logistics.Pricing.Handlers;

public static class PricingHandlers
{
    public static ValueTask Handle(QuoteShipment command, ConsumeContext context)
    {
        var baseRate = 2.0m;
        var perKg = 0.5m;
        var multiplier = command.ServiceLevel switch
        {
            "express" => 2.0m,
            "next-day" => 1.5m,
            _ => 1.0m,
        };
        var amount = (baseRate + perKg * command.WeightGrams / 1000m) * multiplier;

        Console.WriteLine($"[pricing] Цитата {command.QuoteId} для заказа {command.OrderId}: {amount:C} ({command.ServiceLevel})");

        return context.PublishAsync(new ShipmentQuoted(command.QuoteId, command.OrderId, amount, command.ServiceLevel));
    }
}