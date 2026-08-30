using AvtoBus;

namespace AvtoBus.QuickStart.Contracts;

// Команда — ожидает ровно один обработчик.
public sealed record PlaceOrder(Guid OrderId, string CustomerId, OrderItem[] Items) : ICommand;

public sealed record OrderItem(string Sku, int Qty, decimal Price);

// Событие — может обрабатываться многими подписчиками.
public sealed record OrderPlaced(Guid OrderId, string CustomerId, decimal Total) : IEvent;
