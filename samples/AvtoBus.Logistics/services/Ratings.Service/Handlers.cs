using AvtoBus;
using AvtoBus.Logistics.Contracts.CustomerService;

namespace Logistics.Ratings.Handlers;

public static class RatingHandlers
{
    public static ValueTask Handle(RateDelivery command, ConsumeContext context)
    {
        Console.WriteLine($"[ratings] {command.ShipmentId} оценено на {command.Score}/5{(command.Comment is null ? "" : $": {command.Comment}")}");

        return context.PublishAsync(new DeliveryRated(command.RatingId, command.ShipmentId, command.Score));
    }
}