using System.Collections.Concurrent;

namespace AvtoBus.ClaimCheck;

/// <summary>
/// In-process blob-store по умолчанию: тела живут в памяти процесса. Подходит для прототипов,
/// тестов и однопроцессных шин; для прод-кластера подключите свою реализацию
/// <see cref="IBlobStore"/> (S3, Azure Blob Storage, сетевой диск).
/// </summary>
public sealed class InMemoryBlobStore : IBlobStore
{
    private readonly ConcurrentDictionary<string, byte[]> _blobs = new(StringComparer.Ordinal);

    public ValueTask<string> PutAsync(byte[] data, CancellationToken ct = default)
    {
        var url = $"mem://{Guid.NewGuid():N}";
        _blobs[url] = data;
        return ValueTask.FromResult(url);
    }

    public ValueTask<byte[]> GetAsync(string url, CancellationToken ct = default)
    {
        if (!_blobs.TryGetValue(url, out var data))
            throw new KeyNotFoundException($"Claim Check: тело {url} не найдено в хранилище.");

        return ValueTask.FromResult(data);
    }

    public ValueTask DeleteAsync(string url, CancellationToken ct = default)
    {
        _blobs.TryRemove(url, out _);
        return ValueTask.CompletedTask;
    }
}
