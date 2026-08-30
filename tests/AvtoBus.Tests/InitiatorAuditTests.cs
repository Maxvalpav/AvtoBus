using AvtoBus.Configuration;
using AvtoBus.Runtime;
using Xunit;

namespace AvtoBus.Tests;

/// <summary>Аудит «кто послал»: инициатор бежит из контекста в заголовок и живёт до конца каскада (идея 332).</summary>
public class InitiatorAuditTests
{
    private static (EnvelopeFactory Factory, BusOptions Options) Create()
    {
        var options = new BusOptions();
        var registry = MessageRegistry.Build([typeof(Contracts.OrderPlaced)]);
        return (new EnvelopeFactory(options, registry, TimeProvider.System), options);
    }

    [Fact]
    public void Root_message_gets_initiator_from_current_context()
    {
        // Не должно утечь в следующий тест, даже если тот забудет обернуть в using.
        Assert.Null(InitiatorContext.Get());

        using var _ = InitiatorContext.Push("user-42");
        var (factory, _) = Create();

        var envelope = factory.Create(new Contracts.OrderPlaced(Guid.NewGuid(), 10m), typeof(Contracts.OrderPlaced), null, null);

        Assert.Equal("user-42", envelope.Header(BusHeaders.Initiator));
    }

    [Fact]
    public void Cascade_inherits_initiator_even_without_context()
    {
        Assert.Null(InitiatorContext.Get());

        using var scope = InitiatorContext.Push("svc-orders");
        var (factory, _) = Create();
        var parent = factory.Create(new Contracts.OrderPlaced(Guid.NewGuid(), 10m), typeof(Contracts.OrderPlaced), null, null);
        scope.Dispose(); // контекст запроса закончился; каскад в фоне

        var child = factory.Create(new Contracts.OrderPaid(Guid.NewGuid()), typeof(Contracts.OrderPaid), null, parent);

        Assert.Equal("svc-orders", child.Header(BusHeaders.Initiator)); ;
    }

    [Fact]
    public void Explicit_header_wins_over_context()
    {
        using var _ = InitiatorContext.Push("user-from-context");
        var (factory, _) = Create();
        var options = new SendOptions().WithHeader(BusHeaders.Initiator, "user-explicit");

        var envelope = factory.Create(new Contracts.OrderPlaced(Guid.NewGuid(), 10m), typeof(Contracts.OrderPlaced), options, null);

        Assert.Equal("user-explicit", envelope.Header(BusHeaders.Initiator));
    }

    [Fact]
    public void Push_restores_previous_value_on_dispose()
    {
        InitiatorContext.Push("outer").Dispose();

        using (InitiatorContext.Push("inner"))
            Assert.Equal("inner", InitiatorContext.Get());

        Assert.Null(InitiatorContext.Get());
    }
}
