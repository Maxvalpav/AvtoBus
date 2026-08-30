using AvtoBus.Configuration;
using AvtoBus.Handlers;
using AvtoBus.Sagas; // For ConsumeContext access via namespace flattening
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AvtoBus.Sagas;

/// <summary>
/// Диспетчер сообщения в durable-сагу: извлекает ключ корреляции и передаёт раннеру
/// (стиль B, §5). Попадает в контракты шины как обычный хендлер.
/// </summary>
internal sealed class DurableSagaDispatcher : IMessageDispatcher
{
    private readonly Type _sagaType;
    private readonly Type _messageType;
    private readonly Func<object, string> _keyAccessor;

    public DurableSagaDispatcher(Type sagaType, Type messageType)
    {
        _sagaType = sagaType;
        _messageType = messageType;
        _keyAccessor = SagaCatalog.KeyAccessorFor(sagaType, messageType);
    }

    public Type MessageType => _messageType;

    public string HandlerName => $"{_sagaType.Name}.{_messageType.Name}";

    public async ValueTask DispatchAsync(ConsumeContext context)
    {
        var runner = context.Services.GetRequiredService<DurableSagaRunner>();
        var key = _keyAccessor(context.Message);
        await runner.DispatchAsync(_sagaType, context.Message, key, context);
    }
}

/// <summary>Регистрация durable-саги (стиль B) в конфигураторе шины.</summary>
public static class DurableSagaRegistration
{
    /// <summary>
    /// Регистрирует durable-сагу: журнал, раннер и по одному диспетчеру на каждый
    /// заявленный тип сообщения (триггер + все сообщения, которые сага ждёт через WaitFor).
    /// </summary>
    public static BusConfigurator AddDurableSaga<TSaga>(this BusConfigurator bus, params Type[] messageTypes)
        where TSaga : class
        => bus.AddDurableSaga(typeof(TSaga), messageTypes);

    /// <summary>Не-generic версия для статических durable-саг (static класс нельзя в generic-параметр).</summary>
    public static BusConfigurator AddDurableSaga(this BusConfigurator bus, Type sagaType, params Type[] messageTypes)
    {
        // Триггер выводим из первого параметра stat-метода Run/Execute, если не указан явно.
        if (messageTypes.Length == 0)
            messageTypes = [SagaCatalog.TriggerType(sagaType)];

        bus.Services.TryAddSingleton<ISagaJournalStore, InMemorySagaJournalStore>();
        bus.Services.TryAddSingleton<DurableSagaRunner>();

        foreach (var messageType in messageTypes.Distinct())
        {
            // Проверяем корреляцию уже здесь — об ошибке узнаём при старте, а не в первом сообщении.
            SagaCatalog.KeyAccessorFor(sagaType, messageType);
            bus.AddDispatcher(new DurableSagaDispatcher(sagaType, messageType));
        }

        return bus;
    }
}
