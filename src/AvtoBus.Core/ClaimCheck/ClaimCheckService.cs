using System.Globalization;
using AvtoBus.Configuration;

namespace AvtoBus.ClaimCheck;

/// <summary>
/// Claim Check (идея 138): большой payload не ездит через брокер. На отправке тело
/// кладётся в blob-store, в конверте остаётся ссылка + размер; на приёме тело
/// выкачивается обратно до десериализации.
///
/// Точки интеграции: исходящая сторона — из send-пути <c>AvtoBusClient</c>,
/// входящая — из <c>MessageProcessor</c> перед <c>Deserialize</c> (в этот момент тело
/// конверта ещё можно заменить; в пайплайне — уже поздно, сообщение десериализовано).
/// </summary>
public sealed class ClaimCheckService(IBlobStore blobs, BusOptions options)
{
    private readonly int _threshold = options.ClaimCheck?.ThresholdBytes ?? 0;

    /// <summary>
    /// На отправке: тела крупнее порога уезжают в blob-store, конверт получает ссылку
    /// и исходный размер, тело очищается.
    /// </summary>
    public async ValueTask<Envelope> ExternalizeAsync(Envelope envelope, CancellationToken ct)
    {
        if (_threshold <= 0 || envelope.Body.Length <= _threshold)
            return envelope;

        var url = await blobs.PutAsync(envelope.Body.ToArray(), ct).ConfigureAwait(false);
        var size = envelope.Body.Length.ToString(CultureInfo.InvariantCulture);

        return envelope
            .WithHeader(ClaimCheckOptions.UrlHeader, url)
            .WithHeader(ClaimCheckOptions.SizeHeader, size)
            with
        { Body = ReadOnlyMemory<byte>.Empty };
    }

    /// <summary>
    /// На приёме: если конверт — claim-check ссылка, выкачивает тело обратно в конверт
    /// до десериализации. Конверты без ссылки не трогает.
    /// </summary>
    public async ValueTask<Envelope> HydrateAsync(Envelope envelope, CancellationToken ct)
    {
        if (envelope.Header(ClaimCheckOptions.UrlHeader) is not { } url)
            return envelope;

        try
        {
            var body = await blobs.GetAsync(url, ct).ConfigureAwait(false);
            if (body.Length == 0) throw new InvalidDataException($"ClaimCheck blob at {url} is empty.");
            return envelope with { Body = body };
        }
        catch (KeyNotFoundException ex)
        {
            throw new InvalidDataException($"ClaimCheck blob not found: {url}", ex);
        }
    }
}
