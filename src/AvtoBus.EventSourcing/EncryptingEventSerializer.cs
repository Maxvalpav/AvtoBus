namespace AvtoBus.EventSourcing;

/// <summary>
/// Обёртка над <see cref="IEventSerializer"/>, применяющая crypto-shredding (идея 264):
/// зашифрованные поля при записи, расшифровка при чтении (или null, если ключ удалён).
/// </summary>
public sealed class EncryptingEventSerializer : IEventSerializer
{
    private readonly IEventSerializer _inner;
    private readonly SubjectDataProtection _protection;

    public EncryptingEventSerializer(IEventSerializer inner, SubjectDataProtection protection)
    {
        _inner = inner;
        _protection = protection;
    }

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Обёртка делегирует аннотированным Protect/Unprotect; включается только при настроенном шифровании.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Aot", "IL3050", Justification =
        "Обёртка делегирует аннотированным Protect/Unprotect; включается только при настроенном шифровании.")]
    public ReadOnlyMemory<byte> Serialize(object @event)
        => _protection.Protect(@event, MessageTypeNaming.NameOf(@event.GetType()));

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Trimming", "IL2026", Justification =
        "Обёртка делегирует аннотированным Protect/Unprotect; включается только при настроенном шифровании.")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Aot", "IL3050", Justification =
        "Обёртка делегирует аннотированным Protect/Unprotect; включается только при настроенном шифровании.")]
    public object Deserialize(ReadOnlyMemory<byte> data, string eventType)
    {
        var clrType = _inner.ResolveType(eventType) ?? throw new UnknownEventTypeException(eventType);
        return _protection.Unprotect(data, eventType, clrType);
    }

    public ReadOnlyMemory<byte> SerializeSnapshot(object state) => _inner.SerializeSnapshot(state);

    public T DeserializeSnapshot<T>(ReadOnlyMemory<byte> data) where T : class
        => _inner.DeserializeSnapshot<T>(data);

    public void RegisterType(string eventType, Type clrType) => _inner.RegisterType(eventType, clrType);

    public Type? ResolveType(string eventType) => _inner.ResolveType(eventType);
}
