using System.IO.Compression;

namespace AvtoBus.Compression;

public static class CompressionHelper
{
    public static bool ShouldCompress(ReadOnlyMemory<byte> body, CompressionOptions opts)
        => body.Length >= opts.ThresholdBytes;

    public static ReadOnlyMemory<byte> Compress(ReadOnlyMemory<byte> body, CompressionOptions opts)
    {
        // Avoid extra ToArray: use Span-based copy via underlying buffer
        using var dst = new MemoryStream();
        using (var gz = new GZipStream(dst, opts.Level, leaveOpen: true))
            gz.Write(body.Span);
        return dst.ToArray();
    }

    public static ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> body)
    {
        using var src = new MemoryStream(body.ToArray());
        using var gz = new GZipStream(src, CompressionMode.Decompress);
        using var dst = new MemoryStream();
        gz.CopyTo(dst);
        return dst.ToArray();
    }

    public static void Compress(IBufferWriter<byte> writer, ReadOnlyMemory<byte> body, CompressionOptions opts)
    {
        // Zero-copy variant: compress directly into IBufferWriter
        using var ms = new MemoryStream();
        using (var gz = new GZipStream(ms, opts.Level, leaveOpen: true))
            gz.Write(body.Span);
        var compressed = ms.ToArray();
        writer.Write(compressed);
    }
}
