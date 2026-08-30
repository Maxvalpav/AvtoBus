using AvtoBus;
using AvtoBus.Logistics.Contracts.CustomerService;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Notifications.Handlers;

public static class NotificationHandlers
{
    public static ValueTask Handle(SendNotification command, ConsumeContext context)
    {
        var externalId = $"MSG-{command.NotificationId:N}";
        Console.WriteLine($"[notifications] {command.Channel}: «{command.Template}» → {command.Destination} (клиент {command.CustomerId})");

        return context.PublishAsync(new NotificationSent(command.NotificationId, command.CustomerId, command.Channel, externalId));
    }

    // Event-driven: Delivery публикует Delivered → клиенту уходит уведомление о вручении.
    public static ValueTask Handle(Delivered delivered, ConsumeContext context)
    {
        Console.WriteLine($"[notifications] email: «delivery_delivered» → клиент по {delivered.ShipmentId} (вручено {delivered.At:HH:mm:ss})");

        return context.PublishAsync(new NotificationSent(Guid.NewGuid(), Guid.Empty, "email", $"MSG-{Guid.NewGuid():N}"));
    }
}