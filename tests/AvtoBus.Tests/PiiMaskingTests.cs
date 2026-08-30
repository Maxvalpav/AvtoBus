using AvtoBus.Contracts;
using AvtoBus.Diagnostics;
using AvtoBus.Testing;
using AvtoBus.Tests.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Tests;

/// <summary>Контракт с метками PII (идея 456).</summary>
public sealed record PatientEvent(
    [property: PersonalData(Category = "name")] string PatientName,
    [property: PersonalData] string Phone,
    Guid RecordId,
    string Procedure) : IEvent;

public class PiiMaskingTests
{

    [Fact]
    public void Pii_fields_are_masked_but_regular_fields_survive()
    {
        var output = PiiMasker.ToMaskedText(new PatientEvent("Иван Петров", "+7 900 123-45-67", Guid.NewGuid(), "MRI"));

        Assert.DoesNotContain("Иван Петров", output, StringComparison.Ordinal);
        Assert.DoesNotContain("+7 900 123-45-67", output, StringComparison.Ordinal);
        Assert.Contains("MRI", output, StringComparison.Ordinal);
        Assert.Contains("\"PatientName\":\"###", output, StringComparison.Ordinal);
        Assert.Contains("\"Phone\":\"###", output, StringComparison.Ordinal);
    }

    [Fact]
    public void Same_input_yields_same_mask_enabling_correlation()
    {
        var a = PiiMasker.Mask("user@example.com");
        var b = PiiMasker.Mask("user@example.com");
        var c = PiiMasker.Mask("other@example.com");

        Assert.Equal(a, b);
        Assert.NotEqual(a, c);
    }

    [Fact]
    public void Null_message_is_safe()
    {
        Assert.Equal("null", PiiMasker.ToMaskedText(null));
    }

    [Fact]
    public async Task Second_line_description_keeps_data_without_pii_markers()
    {
        // Контрастный тест: PlaceOrder не размечает поля [PersonalData],
        // поэтому даже при включённом маскировании данные попадают в описание.
        var received = new TaskCompletionSource<IFailed<PlaceOrder>>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus =>
            {
                bus.AddConsumer<PlaceOrderConsumer>();
                bus.AddConsumer<FailedPlaceOrderObserver>();
                bus.Recoverability(r => r.ImmediateRetries(0).DelayedRetries(0));
                bus.PiiMaskingEnabled = true;
            },
            services => services.AddSingleton(received));
        harness.Faults.FailNext<PlaceOrder>();

        await harness.Bus.SendAsync(new PlaceOrder(Guid.NewGuid(), "customer-secret-id", 99m));

        var failed = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Contains("customer-secret-id", failed.ErrorDescription, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Second_line_description_redacts_pii_when_masking_is_enabled()
    {
        var received = new TaskCompletionSource<IFailed<PatientEvent>>();

        await using var harness = await AvtoBusTestHarness.StartAsync(
            bus =>
            {
                bus.AddConsumer<PatientEventConsumer>();
                bus.AddConsumer<FailedPatientObserver>();
                bus.Recoverability(r => r.ImmediateRetries(0).DelayedRetries(0));
                bus.PiiMaskingEnabled = true;
            },
            services => services.AddSingleton(received));
        harness.Faults.FailNext<PatientEvent>();

        await harness.Bus.PublishAsync(new PatientEvent("Иван Петров", "+7 900 123-45-67", Guid.NewGuid(), "MRI"));

        var failed = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.DoesNotContain("Иван Петров", failed.ErrorDescription, StringComparison.Ordinal);
        Assert.Contains("MRI", failed.ErrorDescription, StringComparison.Ordinal);
    }
}

public sealed class PatientEventConsumer : IConsumer<PatientEvent>
{
    public Task ConsumeAsync(ConsumeContext<PatientEvent> context)
        => throw new InvalidOperationException("сбой");
}

public sealed class FailedPatientObserver(TaskCompletionSource<IFailed<PatientEvent>> signal) : IFailedConsumer<PatientEvent>
{
    public Task ConsumeAsync(IFailed<PatientEvent> failed, ConsumeContext context)
    {
        signal.TrySetResult(failed);
        return Task.CompletedTask;
    }
}

public sealed class PlaceOrderConsumer : IConsumer<PlaceOrder>
{
    public Task ConsumeAsync(ConsumeContext<PlaceOrder> context)
        => throw new InvalidOperationException("отмена");
}

public sealed class FailedPlaceOrderObserver(TaskCompletionSource<IFailed<PlaceOrder>> signal) : IFailedConsumer<PlaceOrder>
{
    public Task ConsumeAsync(IFailed<PlaceOrder> failed, ConsumeContext context)
    {
        signal.TrySetResult(failed);
        return Task.CompletedTask;
    }
}
