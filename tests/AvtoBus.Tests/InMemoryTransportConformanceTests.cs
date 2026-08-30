using AvtoBus.InMemory;

namespace AvtoBus.Tests;

/// <summary>Conformance-прогон для in-memory брокера (док 18 §7, идея 98).</summary>
public sealed class InMemoryTransportConformanceTests : TransportConformanceTests
{
    protected override Task<ITransport> CreateAsync() => Task.FromResult<ITransport>(new InMemoryTransport());
}
