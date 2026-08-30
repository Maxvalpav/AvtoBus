namespace AvtoBus.ClaimCheck;

/// <summary>
/// Настройки Claim Check (идея 138): порог, с которого тело уезжает в blob-store.
/// </summary>
public sealed class ClaimCheckOptions
{
    /// <summary>Заголовок со ссылкой на тело в blob-store (binary Claim Check).</summary>
    public const string UrlHeader = "avtobus.claim-check";

    /// <summary>Заголовок с исходным размером тела — для аудита и лимитов (в байтах).</summary>
    public const string SizeHeader = "avtobus.claim-check-size";

    /// <summary>Тела крупнее порога уходят в blob-store, в брокер — только ссылка.</summary>
    public int ThresholdBytes { get; set; } = 256 * 1024;
}
