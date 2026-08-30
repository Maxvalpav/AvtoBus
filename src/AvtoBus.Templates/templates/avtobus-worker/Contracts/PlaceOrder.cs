namespace MyWorker.Contracts;

/// <summary>Пример команды: создание заказа.</summary>
public record PlaceOrder : AvtoBus.ICommand
{
    public required string OrderId { get; init; }
    public decimal Total { get; init; }
}