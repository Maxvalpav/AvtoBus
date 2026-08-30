namespace MyWorker.Contracts;

/// <summary>Пример события: заказ размещён.</summary>
public record OrderPlaced : AvtoBus.IEvent
{
    public required string OrderId { get; init; }
    public decimal Total { get; init; }
}