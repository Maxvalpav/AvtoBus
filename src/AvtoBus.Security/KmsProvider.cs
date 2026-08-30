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
    private readonly Dictionary<string, List<byte[]>> _keys = new();
    public void AddKey(string keyId, byte[] key)
    {
        if (!_keys.ContainsKey(keyId)) _keys[keyId] = new List<byte[]>();
        _keys[keyId].Add(key);
    }
    public Task<byte[]> GetCurrentKeyAsync(string keyId, CancellationToken ct)
        => Task.FromResult(_keys.TryGetValue(keyId, out var list) && list.Count > 0 ? list[^1] : RandomNumberGenerator.GetBytes(32));
    public Task<byte[]> GetKeyByVersionAsync(string keyId, int version, CancellationToken ct)
        => Task.FromResult(_keys.TryGetValue(keyId, out var list) && version < list.Count ? list[version] : RandomNumberGenerator.GetBytes(32));
}

public sealed class KmsKeyRing
{
    private readonly IKmsProvider _kms;
    private readonly string _keyId;
    public KmsKeyRing(IKmsProvider kms, string keyId) { _kms = kms; _keyId = keyId; }
    public Task<byte[]> CurrentAsync(CancellationToken ct) => _kms.GetCurrentKeyAsync(_keyId, ct);
    public Task<byte[]> VersionAsync(int v, CancellationToken ct) => _kms.GetKeyByVersionAsync(_keyId, v, ct);
}
