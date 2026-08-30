using System.Text.Json.Serialization;

namespace AvtoBus.Serialization;

/// <summary>
/// STJ-контекст внутренних wire-типов Core (идея 454): статически известные типы
/// сериализуются через <see cref="JsonTypeInfo"/> без рефлексии — AOT-safe.
/// Используется <see cref="AvtoBus.PrincipalSerializer"/>.
/// </summary>
[JsonSerializable(typeof(AvtoBus.UserWire))]
[JsonSerializable(typeof(AvtoBus.ClaimWire))]
internal sealed partial class AvtoBusCoreJsonContext : JsonSerializerContext;
