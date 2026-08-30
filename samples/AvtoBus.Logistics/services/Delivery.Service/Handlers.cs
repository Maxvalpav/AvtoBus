using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Delivery.Handlers;

public static class DeliveryHandlers
{
    public static ValueTask Handle(AttemptDelivery command, ConsumeContext context)
    {
        // Попытка 1 имитирует неудачу — дальше вступают отложенные ретраи, попытка 2 успешна.
        if (context.Attempt <= 1)
            throw new InvalidOperationException($"Получатель не открыл дверь ({context.Attempt}-я попытка)");

        Console.WriteLine($"[delivery] {command.ShipmentId} вручено в {command.At:HH:mm:ss} (попытка {context.Attempt})");
        return context.PublishAsync(new Delivered(command.ShipmentId, command.At));
    }

    public static ValueTask Handle(RescheduleDelivery command, ConsumeContext context)
    {
        Console.WriteLine($"[delivery] {command.ShipmentId} перенесено на {command.NewWindowStart:HH:mm:ss}: «{command.Reason}»");
        return context.PublishAsync(new DeliveryRescheduled(command.ShipmentId, command.NewWindowStart, command.Reason));
    }
}