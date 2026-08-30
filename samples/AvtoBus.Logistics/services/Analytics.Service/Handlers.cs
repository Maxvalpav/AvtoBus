using AvtoBus;
using AvtoBus.Logistics.Contracts.CustomerService;
using AvtoBus.Logistics.Contracts.Ordering;

namespace Logistics.Analytics.Handlers;

public static class AnalyticsHandlers
{
    public static ValueTask Handle(RecordAnalytics command, ConsumeContext context)
    {
        Console.WriteLine($"[analytics] {command.Metric} = {command.Value} ({command.Dimension})");

        return context.PublishAsync(new AnalyticsRecorded(command.EventId, command.Metric, command.Value));
    }

    // Event-driven: Orders публикует OrderPlaced → аналитика сама считает метрику заказов.
    public static ValueTask Handle(OrderPlaced placed, ConsumeContext context)
    {
        Console.WriteLine($"[analytics] orders.created = 1 (клиент {placed.CustomerId}, сумма {placed.Total:C})");

        return context.PublishAsync(new AnalyticsRecorded(Guid.NewGuid(), "orders.created", 1));
    }
}