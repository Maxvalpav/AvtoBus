using AvtoBus;
using AvtoBus.Logistics.Contracts.Finance;

namespace Logistics.Returns.Handlers;

public static class ReturnHandlers
{
    public static ValueTask Handle(InitiateReturn command, ConsumeContext context)
    {
        var rma = $"RMA-{command.ReturnId:N}";
        Console.WriteLine($"[returns] Возврат {rma} по заказу {command.OrderId}: «{command.Reason}»");

        return context.PublishAsync(new ReturnInitiated(command.ReturnId, command.OrderId, rma));
    }

    public static ValueTask Handle(CompleteReturn command, ConsumeContext context)
    {
        Console.WriteLine($"[returns] Возврат {command.RmaNumber} по заказу {command.OrderId} завершён (товар принят на складе)");

        return context.PublishAsync(new ReturnCompleted(command.ReturnId, command.OrderId, command.RmaNumber));
    }
}