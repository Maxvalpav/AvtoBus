namespace AvtoBus.Abstractions;

public abstract class AvtoSaga
{
    public string? SagaId { get; set; }
    public long Version { get; set; }
}

public interface IAvtoSagaDescriptor
{
    string SagaType { get; }
    Type SagaClrType { get; }
    IReadOnlyList<Type> MessageTypes { get; }
    bool CanStart(Type messageType);
    string GetCorrelationId(object message);
    AvtoEffects InvokeStart(AvtoSaga saga, object message);
    AvtoEffects InvokeHandle(AvtoSaga saga, object message);
}
