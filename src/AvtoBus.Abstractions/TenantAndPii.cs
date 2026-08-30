namespace AvtoBus.Abstractions;

public interface ITenantResolver
{
    string? ResolveTenant(AvtoEnvelope envelope);
}

public sealed class HeaderTenantResolver : ITenantResolver
{
    public string? ResolveTenant(AvtoEnvelope envelope)
        => envelope.TenantId ?? (envelope.Headers.TryGetValue("tenant-id", out var v) ? v : null);
}

public interface IPiiMasker
{
    string Mask(string value);
    IReadOnlySet<string> SensitiveHeaders { get; }
}

public sealed class DefaultPiiMasker : IPiiMasker
{
    public IReadOnlySet<string> SensitiveHeaders => new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "authorization", "cookie", "x-api-key", "tenant-id", "avto-tenant-id",
    };

    public string Mask(string value)
        => value.Length <= 4 ? "****" : value[..2] + new string('*', value.Length - 4) + value[^2..];
}

public sealed record ClaimCheckReference(string ClaimId, string Store, string? Uri = null);

public interface IClaimCheckStore
{
    ValueTask<ClaimCheckReference> StoreAsync(byte[] payload, CancellationToken ct);
    ValueTask<byte[]?> RetrieveAsync(ClaimCheckReference reference, CancellationToken ct);
}

public sealed class InMemoryClaimCheckStore : IClaimCheckStore
{
    private readonly Dictionary<string, byte[]> _store = new();
    public ValueTask<ClaimCheckReference> StoreAsync(byte[] payload, CancellationToken ct)
    {
        var id = Guid.NewGuid().ToString("N");
        _store[id] = payload;
        return ValueTask.FromResult(new ClaimCheckReference(id, "memory", $"claim://{id}"));
    }
    public ValueTask<byte[]?> RetrieveAsync(ClaimCheckReference reference, CancellationToken ct)
        => ValueTask.FromResult(_store.GetValueOrDefault(reference.ClaimId));
}
