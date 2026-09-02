using AvtoBus.Compression;

namespace AvtoBus.Pipeline;

/// <summary>Сжатие тел &gt; threshold (gzip): на входе разжимает, тем самым хендлер видит оригинал (идея 105).</summary>
public sealed class CompressionMiddleware : IBusMiddleware
{
    private readonly CompressionOptions _options;

    public CompressionMiddleware(CompressionOptions options) => _options = options;

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        if (ctx.Envelope.Headers.TryGetValue(BusHeaders.ContentEncoding, out var enc) && enc == "gzip")
        {
            try
            {
                var decompressed = CompressionHelper.Decompress(ctx.Envelope.Body, _options.MaxDecompressedBytes);
                ctx.ReplaceEnvelope(ctx.Envelope with { Body = decompressed, Headers = RemoveEncoding(ctx.Envelope.Headers) });
            }
            catch (InvalidDataException ex)
            {
                ctx.DeadLetter($"decompression failed: {ex.Message}");
                return;
            }
            catch
            {
                // Corrupted gzip — let handler fail and go to DLQ via normal recoverability
            }
        }
        await next(ctx).ConfigureAwait(false);
    }

    private static System.Collections.Frozen.FrozenDictionary<string, string> RemoveEncoding(IReadOnlyDictionary<string, string> headers)
    {
        var d = new Dictionary<string, string>(headers, StringComparer.Ordinal);
        d.Remove(BusHeaders.ContentEncoding);
        return System.Collections.Frozen.FrozenDictionary.ToFrozenDictionary(d, StringComparer.Ordinal);
    }
}
