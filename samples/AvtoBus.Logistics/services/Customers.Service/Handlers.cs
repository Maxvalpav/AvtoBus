using AvtoBus;
using AvtoBus.Logistics.Contracts.Ordering;

namespace Logistics.Customers.Handlers;

public static class CustomerHandlers
{
    public static ValueTask Handle(RegisterCustomer command, ConsumeContext context)
    {
        Console.WriteLine($"[customers] Клиент {command.CustomerId}: {command.Name} <{command.Email}>");

        return context.PublishAsync(new CustomerRegistered(command.CustomerId, command.Name, command.Email));
    }
}