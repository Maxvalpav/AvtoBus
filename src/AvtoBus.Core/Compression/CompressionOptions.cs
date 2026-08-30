namespace AvtoBus.Compression;

/// <summary>Сжатие тел сообщений (идея 105 доп): GZip для &gt; threshold, заголовок avtobus-compression.</summary>
public sealed class CompressionOptions
{
    public const string Header = "avtobus-compression";

    public const string Gzip = "gzip";

    /// <summary>Тела меньше порога не сжимаем — накладные расходы больше выигрыша.</summary>
    public int ThresholdBytes { get; set; } = 1024;

    /// <summary>Уровень: Optimal vs Fastest.</summary>
    public System.IO.Compression.CompressionLevel Level { get; set; } = System.IO.Compression.CompressionLevel.Optimal;
}
