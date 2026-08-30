using AvtoBus;
using AvtoBus.Logistics.Contracts.Finance;

namespace Logistics.Invoices.Handlers;

public static class InvoiceHandlers
{
    public static ValueTask Handle(GenerateInvoice command, ConsumeContext context)
    {
        var number = $"INV-{DateTimeOffset.UtcNow:yyyyMMdd}-{command.InvoiceId:N}".ToUpperInvariant();
        Console.WriteLine($"[invoices] Счёт {number} для заказа {command.OrderId} на {command.Amount:C}");

        return context.PublishAsync(new InvoiceGenerated(command.InvoiceId, command.OrderId, number, command.Amount));
    }
}