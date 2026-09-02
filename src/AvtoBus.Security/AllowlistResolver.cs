using AvtoBus;
using AvtoBus.Configuration;

namespace AvtoBus.Security;

/// <summary>Allowlist-реализация ITypeResolver: только разрешённые типы резолвятся (fail-closed).</summary>
public sealed class AllowlistResolver(BusOptions busOptions, MessageRegistry registry) : ITypeResolver
{
    public bool TryResolve(string messageType, out Type type)
    {
        if (busOptions.AllowedMessageTypes is { Count: >0 } allow && !allow.Contains(messageType))
        {
            type = null!;
            return false;
        }
        if (registry.TryResolve(messageType, out var t)) { type = t!; return true; }
        type = null!; return false;
    }
    public string NameOf(Type type) => registry.NameOf(type);
}

/// <summary>Документированный пример из docs/code/17 — алиас для совместимости.</summary>
public sealed class AllowlistTypeResolver : ITypeResolver
{
    private readonly AllowlistResolver _inner;
    public AllowlistTypeResolver(BusOptions opts, MessageRegistry reg) => _inner = new AllowlistResolver(opts, reg);
    public bool TryResolve(string messageType, out Type type) => _inner.TryResolve(messageType, out type);
    public string NameOf(Type type) => _inner.NameOf(type);
}
