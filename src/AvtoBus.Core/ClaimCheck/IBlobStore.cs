namespace AvtoBus.ClaimCheck;

/// <summary>
/// Хранилище больших payload'ов для Claim Check (идея 138): в брокер уходит только ссылка,
/// само тело — в blob-store (S3/Azure Blob/файловое хранилище).
/// Реализацию предоставляет приложение через <c>UseClaimCheck</c>; без неё используется
/// <see cref="InMemoryBlobStore"/> (годен для прототипов и тестов, не для прод-кластера).
/// </summary>
public interface IBlobStore
{
    /// <summary>Сохраняет тело и возвращает ссылку, которая уедет в заголовок конверта.</summary>
    ValueTask<string> PutAsync(byte[] data, CancellationToken ct = default);

    /// <summary>Возвращает тело по ссылке из заголовка <c>avtobus.claim-check</c>.</summary>
    ValueTask<byte[]> GetAsync(string url, CancellationToken ct = default);

    /// <summary>Удаляет тело, когда сообщение больше не нужно (retention).</summary>
    ValueTask DeleteAsync(string url, CancellationToken ct = default);
}
