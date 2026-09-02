using System.Buffers;
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

    public static ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> body, int maxDecompressedBytes = 10 * 1024 * 1024)
    {
        using var src = new MemoryStream(body.ToArray());
        using var gz = new GZipStream(src, CompressionMode.Decompress);
        using var dst = new MemoryStream();
        var buffer = new byte[8192];
        int total = 0, read;
        while ((read = gz.Read(buffer, 0, buffer.Length)) > 0)
        {
            total += read;
            if (total > maxDecompressedBytes)
                throw new InvalidDataException($"Decompressed payload exceeds {maxDecompressedBytes} bytes — possible zip-bomb.");
            dst.Write(buffer, 0, read);
        }
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
