using AvtoBus.QuickStart.Contracts;
using AvtoBus.QuickStart.Data;

namespace AvtoBus.QuickStart.Handlers;

// Статический класс + статический метод — ноль церемоний.
public static class OrderHandlers
{
    // OrderDbContext инжектится по параметру из scoped DI, CancellationToken — из consume context.
    // Возврат OrderPlaced публикуется каскадно через Outbox (одна транзакция с бизнес-данными).
    public static async Task<OrderPlaced> Handle(PlaceOrder cmd, OrderDbContext db, CancellationToken ct)
    {
        var order = new Order
        {
            Id = cmd.OrderId,
            CustomerId = cmd.CustomerId,
            Items = cmd.Items.ToList(),
        };
        db.Orders.Add(order);
        await db.SaveChangesAsync(ct); // order + outbox-запись в одной транзакции
        return new OrderPlaced(cmd.OrderId, cmd.CustomerId, order.Total);
    }
}
