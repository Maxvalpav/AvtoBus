using System.IO.Compression;

namespace AvtoBus.Compression;

public static class CompressionHelper
{
    public static bool ShouldCompress(ReadOnlyMemory<byte> body, CompressionOptions opts)
        => body.Length >= opts.ThresholdBytes;

    public static ReadOnlyMemory<byte> Compress(ReadOnlyMemory<byte> body, CompressionOptions opts)
    {
        using var src = new MemoryStream(body.ToArray());
        using var dst = new MemoryStream();
        using (var gz = new GZipStream(dst, opts.Level, leaveOpen: true))
            src.CopyTo(gz);
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
}
