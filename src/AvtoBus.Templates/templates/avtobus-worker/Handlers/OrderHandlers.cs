using AvtoBus;
using MyWorker.Contracts;

namespace MyWorker.Handlers;

/// <summary>Консьюмер по конвенции: статический метод Handle с первым параметром — сообщением.</summary>
public static class OrderHandlers
{
    public static ValueTask Handle(PlaceOrder command, ConsumeContext context)
    {
        Console.WriteLine($"[order] Получена команда {command.OrderId} на сумму {command.Total}");
        return context.PublishAsync(new OrderPlaced { OrderId = command.OrderId, Total = command.Total });
    }

    public static ValueTask Handle(OrderPlaced @event, ConsumeContext context)
    {
        Console.WriteLine($"[order] Событие { @event.OrderId } размещено");
        return ValueTask.CompletedTask;
    }
}