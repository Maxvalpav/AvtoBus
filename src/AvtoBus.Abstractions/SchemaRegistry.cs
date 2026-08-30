namespace AvtoBus.Abstractions;

public enum SchemaCompatibility { Active, Deprecated, Incompatible }

public sealed record SchemaDescriptor(
    string SchemaName,
    int Version,
    string MessageType,
    SchemaCompatibility Compatibility,
    string ContentType,
    string? JsonSchema = null,
    string? PayloadHash = null);

public delegate object Upcaster(object oldPayload);

public interface ISchemaRegistry
{
    void Register(SchemaDescriptor descriptor);
    SchemaDescriptor? Lookup(string schemaName, int version);
    SchemaDescriptor? Latest(string schemaName);
    void AddUpcaster(string schemaName, int fromVersion, int toVersion, Upcaster upcaster);
    object Upcast(object payload, string schemaName, int fromVersion, int toVersion);
    IReadOnlyList<SchemaDescriptor> All { get; }
}

public sealed class InMemorySchemaRegistry : ISchemaRegistry
{
    private readonly Dictionary<(string, int), SchemaDescriptor> _schemas = new();
    private readonly Dictionary<(string, int, int), Upcaster> _upcasters = new();

    public void Register(SchemaDescriptor descriptor)
        => _schemas[(descriptor.SchemaName, descriptor.Version)] = descriptor;

    public SchemaDescriptor? Lookup(string schemaName, int version)
        => _schemas.GetValueOrDefault((schemaName, version));

    public SchemaDescriptor? Latest(string schemaName)
        => _schemas.Values.Where(s => s.SchemaName == schemaName).MaxBy(s => s.Version);

    public void AddUpcaster(string schemaName, int fromVersion, int toVersion, Upcaster upcaster)
        => _upcasters[(schemaName, fromVersion, toVersion)] = upcaster;

    public object Upcast(object payload, string schemaName, int fromVersion, int toVersion)
    {
        var current = payload;
        for (var v = fromVersion; v < toVersion; v++)
        {
            if (_upcasters.TryGetValue((schemaName, v, v + 1), out var up))
                current = up(current);
            else
                throw new InvalidOperationException($"No upcaster {schemaName} v{v}->v{v + 1}");
        }
        return current;
    }

    public IReadOnlyList<SchemaDescriptor> All => _schemas.Values.ToList();
}

public static class SchemaRegistryExtensions
{
    public static void RegisterMessage<T>(this ISchemaRegistry registry)
    {
        var type = typeof(T);
        var name = (string?)type.GetProperty("SchemaName", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)?.GetValue(null)
            ?? type.FullName!;
        var version = (int?)type.GetProperty("SchemaVersion", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)?.GetValue(null)
            ?? 1;
        registry.Register(new SchemaDescriptor(name, version, type.FullName!, SchemaCompatibility.Active, "application/json"));
    }
}
