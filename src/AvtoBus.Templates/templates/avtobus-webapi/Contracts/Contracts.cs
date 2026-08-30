namespace MyWebApi.Contracts;

/// <summary>Команда из API: создание заказа.</summary>
public record PlaceOrder : AvtoBus.ICommand
{
    public required string OrderId { get; init; }
    public decimal Total { get; init; }
}

/// <summary>Событие, публикуемое консьюмером.</summary>
public record OrderPlaced : AvtoBus.IEvent
{
    public required string OrderId { get; init; }
}