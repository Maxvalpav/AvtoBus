using Microsoft.CodeAnalysis;

namespace AvtoBus.Analyzers;

public static class Rules
{
    // ── Commands & Events ──

    public static readonly DiagnosticDescriptor AVB001 = new(
        "AVB001", "No handler for command",
        "Command '{0}' is sent via bus.Send() but no handler (Handle/Consume) is registered for it",
        "AvtoBus.Routing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB001");

    public static readonly DiagnosticDescriptor AVB002 = new(
        "AVB002", "Multiple handlers for command",
        "Command '{0}' has {1} handlers — commands must have exactly one handler",
        "AvtoBus.Routing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB002");

    public static readonly DiagnosticDescriptor AVB003 = new(
        "AVB003", "Event has no subscribers",
        "Event '{0}' is published but no consumer subscribes — consider adding a handler or removing the publish",
        "AvtoBus.Routing", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB003");

    public static readonly DiagnosticDescriptor AVB004 = new(
        "AVB004", "Publish used for ICommand",
        "'{0}' implements ICommand but is sent via Publish() — use Send() for commands",
        "AvtoBus.Routing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB004");

    public static readonly DiagnosticDescriptor AVB005 = new(
        "AVB005", "Send used for IEvent",
        "'{0}' implements IEvent but is sent via Send() — use Publish() for events",
        "AvtoBus.Routing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB005");

    public static readonly DiagnosticDescriptor AVB008 = new(
        "AVB008", "First parameter is not a message contract",
        "First parameter '{0}' of a handler is not a message contract; use a concrete command/event type",
        "AvtoBus.Routing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB008");

    // ── Contracts ──

    public static readonly DiagnosticDescriptor AVB010 = new(
        "AVB010", "Mutable contract",
        "Message contract '{0}' has settable properties — use 'init' or 'required init' for immutability",
        "AvtoBus.Contracts", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB010");

    public static readonly DiagnosticDescriptor AVB011 = new(
        "AVB011", "Contract references domain type",
        "Contract '{0}' references domain type '{1}' — contracts must be standalone DTOs",
        "AvtoBus.Contracts", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB011");

    public static readonly DiagnosticDescriptor AVB015 = new(
        "AVB015", "Decimal without currency",
        "Property '{0}.{1}' is decimal without [Currency] — financial amounts need explicit currency",
        "AvtoBus.Contracts", DiagnosticSeverity.Info, true,
        helpLinkUri: "https://avtobus.dev/e/AVB015");

    public static readonly DiagnosticDescriptor AVB017 = new(
        "AVB017", "TenantId in contract body",
        "Property 'TenantId' found in contract '{0}' — tenant should be in Envelope.TenantId, not body",
        "AvtoBus.Contracts", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB017");

    public static readonly DiagnosticDescriptor AVB020 = new(
        "AVB020", "Large contract",
        "Contract '{0}' serializes to ~{1} KB — consider using Claim Check pattern for payloads > 64KB",
        "AvtoBus.Contracts", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB020");

    public static readonly DiagnosticDescriptor AVB022 = new(
        "AVB022", "God event",
        "Event '{0}' has {1} properties and name '*Updated' — consider splitting into specific domain events",
        "AvtoBus.Contracts", DiagnosticSeverity.Info, true,
        helpLinkUri: "https://avtobus.dev/e/AVB022");

    // ── Sagas ──

    public static readonly DiagnosticDescriptor AVB040 = new(
        "AVB040", "Saga queries external service",
        "Saga '{0}' calls external service '{1}' — carry data in events instead",
        "AvtoBus.Sagas", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB040");

    public static readonly DiagnosticDescriptor AVB041 = new(
        "AVB041", "Saga without timeout",
        "Saga '{0}' sends command '{1}' but has no timeout — add RequestTimeout to prevent stuck sagas",
        "AvtoBus.Sagas", DiagnosticSeverity.Warning, true,
        helpLinkUri: "https://avtobus.dev/e/AVB041");

    // ── Event Sourcing ──

    public static readonly DiagnosticDescriptor AVB050 = new(
        "AVB050", "Cross-aggregate read",
        "Aggregate '{0}' loads another aggregate '{1}' — read from a projection or listen for events",
        "AvtoBus.EventSourcing", DiagnosticSeverity.Error, true,
        helpLinkUri: "https://avtobus.dev/e/AVB050");

    // ── Naming ──

    public static readonly DiagnosticDescriptor AVB060 = new(
        "AVB060", "Event not past tense",
        "Event '{0}' should be named in past tense (e.g., 'OrderPlaced' not 'PlaceOrderEvent')",
        "AvtoBus.Naming", DiagnosticSeverity.Info, true,
        helpLinkUri: "https://avtobus.dev/e/AVB060");
}
