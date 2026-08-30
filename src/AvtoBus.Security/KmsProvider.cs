using System.Security.Cryptography;

namespace AvtoBus.Security;

/// <summary>
/// Фича 6 по порядку — KMS (как AWS KMS / Azure KeyVault) для ротации per-field ключей.
/// Мощнее Rebus (body only) и NServiceBus (property) — у нас KMS + per-field AES-GCM.
/// </summary>
public interface IKmsProvider
{
    Task<byte[]> GetCurrentKeyAsync(string keyId, CancellationToken ct);
    Task<byte[]> GetKeyByVersionAsync(string keyId, int version, CancellationToken ct);
}

public sealed class InMemoryKmsProvider : IKmsProvider
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, List<byte[]>> _keys = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    public void AddKey(string keyId, byte[] key)
    {
        lock (_gate)
        {
            var list = _keys.GetOrAdd(keyId, _ => new List<byte[]>());
            lock (list) list.Add(key);
        }
    }
    public Task<byte[]> GetCurrentKeyAsync(string keyId, CancellationToken ct)
        => _keys.TryGetValue(keyId, out var list) && list.Count > 0
            ? Task.FromResult(list[^1])
            : Task.FromException<byte[]>(new KeyNotFoundException($"KMS key '{keyId}' not found"));

    public Task<byte[]> GetKeyByVersionAsync(string keyId, int version, CancellationToken ct)
        => _keys.TryGetValue(keyId, out var list) && version >= 0 && version < list.Count
            ? Task.FromResult(list[version])
            : Task.FromException<byte[]>(new KeyNotFoundException($"KMS key '{keyId}' version {version} not found"));
}

public sealed class KmsKeyRing
{
    private readonly IKmsProvider _kms;
    private readonly string _keyId;
    public KmsKeyRing(IKmsProvider kms, string keyId) { _kms = kms; _keyId = keyId; }
    public Task<byte[]> CurrentAsync(CancellationToken ct) => _kms.GetCurrentKeyAsync(_keyId, ct);
    public Task<byte[]> VersionAsync(int v, CancellationToken ct) => _kms.GetKeyByVersionAsync(_keyId, v, ct);
}
