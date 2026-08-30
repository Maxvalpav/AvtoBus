using AvtoBus;
using MyWebApi.Contracts;

namespace MyWebApi.Handlers;

/// <summary>Консьюмер по конвенции: статический метод Handle с первым параметром — сообщением.</summary>
public static class OrderHandlers
{
    public static ValueTask Handle(PlaceOrder command, ConsumeContext context)
    {
        Console.WriteLine($"[orders] Принята команда {command.OrderId}");
        return context.PublishAsync(new OrderPlaced { OrderId = command.OrderId });
    }

    public static ValueTask Handle(OrderPlaced @event, ConsumeContext context)
    {
        Console.WriteLine($"[orders] Заказ { @event.OrderId } размещён");
        return ValueTask.CompletedTask;
    }
}