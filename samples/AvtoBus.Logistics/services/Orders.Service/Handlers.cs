using AvtoBus;
using AvtoBus.Logistics.Contracts.Ordering;

namespace Logistics.Orders.Handlers;

public static class OrderHandlers
{
    public static ValueTask Handle(PlaceOrder command, ConsumeContext context)
    {
        var total = command.Lines.Sum(l => l.Quantity * l.UnitPrice);
        Console.WriteLine($"[orders] Принят заказ {command.OrderId} от клиента {command.CustomerId}, сумма {total:C}");

        return context.PublishAsync(new OrderPlaced(command.OrderId, command.CustomerId, total), new PublishOptions
        {
            CorrelationId = command.CorrelationId,
        });
    }

    public static ValueTask Handle(CancelOrder command, ConsumeContext context)
    {
        Console.WriteLine($"[orders] Заказ {command.OrderId} отменён: «{command.Reason}»");

        return context.PublishAsync(new OrderCancelled(command.OrderId, command.Reason), new PublishOptions
        {
            CorrelationId = command.CorrelationId,
        });
    }
}