using AvtoBus;
using AvtoBus.Logistics.Contracts.Finance;

namespace Logistics.Payments.Handlers;

public static class PaymentHandlers
{
    // Демонстрация маршрутизации ошибок: суммы кратные 1000 «отклоняются».
    private static bool IsDeclined(decimal amount) => amount % 1000m == 0;

    public static async ValueTask Handle(ProcessPayment command, ConsumeContext context)
    {
        if (IsDeclined(command.Amount))
        {
            Console.WriteLine($"[payments] Платёж {command.PaymentId} отклонён ({command.Amount:C})");
            await context.PublishAsync(new PaymentFailed(command.PaymentId, command.OrderId, command.Amount, "card_declined"));
            return;
        }

        Console.WriteLine($"[payments] Платёж {command.PaymentId} проведён ({command.Amount:C}, {command.Method})");
        await context.PublishAsync(new PaymentSucceeded(command.PaymentId, command.OrderId, command.Amount));
    }

    public static ValueTask Handle(RefundPayment command, ConsumeContext context)
    {
        Console.WriteLine($"[payments] Возврат средств {command.RefundId} за заказ {command.OrderId}: {command.Amount:C} («{command.Reason}»)");

        return context.PublishAsync(new PaymentRefunded(command.RefundId, command.OrderId, command.Amount));
    }
}