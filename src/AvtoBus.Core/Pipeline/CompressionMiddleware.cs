using AvtoBus.Compression;

namespace AvtoBus.Pipeline;

/// <summary>Сжатие тел &gt; threshold (gzip): на выходе сжимает, на входе разжимает (идея 105).</summary>
public sealed class CompressionMiddleware : IBusMiddleware
{
    private readonly CompressionOptions _options;

    public CompressionMiddleware(CompressionOptions options) => _options = options;

    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        // Сжатие/разжатие делается в EnvelopeFactory/Serializer, здесь только пропуск
        await next(ctx).ConfigureAwait(false);
    }
}
