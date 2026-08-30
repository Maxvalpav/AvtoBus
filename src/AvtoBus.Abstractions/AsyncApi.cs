namespace AvtoBus.Abstractions;

public sealed record AsyncApiInfo(string Title, string Version, string Description = "");

public interface IAsyncApiExporter
{
    string Export(AsyncApiInfo info);
    IReadOnlyDictionary<string, string> ExportJsonSchemas();
}
