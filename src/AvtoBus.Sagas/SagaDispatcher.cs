using AvtoBus.Handlers;
using Microsoft.Extensions.DependencyInjection;

namespace AvtoBus.Sagas;

/// <summary>
/// Диспетчер одного коррелируемого сообщения в сагу (док 17, §1).
/// Оркестрация per-message: load —> Handle —> инварианты —> save/complete.
/// Работает через <see cref="IMessageDispatcher"/>, поэтому попадает в контракты шины.
/// </summary>
internal sealed class SagaDispatcher<TSaga, TState> : IMessageDispatcher
    where TSaga : Saga<TState>, new()
    where TState : SagaState, new()
{
    private readonly SagaMetadata<TSaga, TState> _meta;
    private readonly SagaMap<TState>.Correlation _correlation;
    private readonly Func<TSaga, object, Task> _invoker;

    public SagaDispatcher(
        SagaMetadata<TSaga, TState> meta,
        SagaMap<TState>.Correlation correlation)
    {
        _meta = meta;
        _correlation = correlation;
        _invoker = meta.InvokerForType(correlation.MessageType);
    }

    public Type MessageType => _correlation.MessageType;

    public string HandlerName => $"{typeof(TSaga).Name}.{MessageType.Name}";

    public async ValueTask DispatchAsync(ConsumeContext context)
    {
        var store = context.Services.GetRequiredService<ISagaStore>();
        var key = _correlation.Key(context.Message);
        var sagaType = typeof(TSaga);

        var loaded = await store.LoadAsync<TState>(sagaType, key, context.CancellationToken)
            .ConfigureAwait(false);

        TSaga saga;
        int expectedVersion;

        if (loaded is null)
        {
            if (!_correlation.StartsNew)
            {
                // Поздний хвост: инстанса нет, а сообщение его не стартует — пропускаем.
                context.Skip($"Сага {typeof(TSaga).Name} не найдена по {key}");
                return;
            }

            saga = context.Services.GetService<TSaga>() ?? new TSaga();
            saga.State = new TState { Id = Guid.CreateVersion7(), CreatedAt = DateTime.UtcNow };
            expectedVersion = 0;
        }
        else
        {
            saga = context.Services.GetService<TSaga>() ?? new TSaga();
            saga.State = loaded.Value.state;
            expectedVersion = loaded.Value.version;
        }

        saga.Context = new SagaContextImpl(context);

        await _invoker(saga, context.Message).ConfigureAwait(false);

        _meta.CheckInvariants(saga.State);

        if (saga.IsComplete)
        {
            await store.CompleteAsync(sagaType, saga.State.Id, context.CancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            await store.SaveAsync(sagaType, key, saga.State, expectedVersion, context.CancellationToken)
                .ConfigureAwait(false);
        }
    }
}
