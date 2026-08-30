using AvtoBus;
using AvtoBus.Logistics.Contracts.Transport;

namespace Logistics.Addresses.Handlers;

public static class AddressHandlers
{
    public static ValueTask Handle(ValidateAddress command, ConsumeContext context)
    {
        var deliverable = !string.IsNullOrWhiteSpace(command.RawAddress) && command.RawAddress.Length > 10;
        var normalized = deliverable ? command.RawAddress.Trim() : command.RawAddress;
        Console.WriteLine($"[addresses] Адрес {command.AddressId}: {(deliverable ? "доставляемый" : "недоставляемый")}");

        return context.PublishAsync(new AddressValidated(command.AddressId, normalized, deliverable));
    }
}