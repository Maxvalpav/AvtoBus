namespace AvtoBus.Abstractions;

public interface IAvtoSerializer
{
    byte[] Serialize(object message);
    object Deserialize(byte[] body, Type messageType);
}

public interface IAvtoMessageTypeRegistry
{
    void Register(Type messageType, string schemaName, int schemaVersion);
    Type? Resolve(string schemaName);
    (string SchemaName, int SchemaVersion) Describe(Type messageType);
}
