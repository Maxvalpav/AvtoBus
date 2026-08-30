using System.Collections.Concurrent;
using System.Text.Json;

namespace AvtoBus.Sagas;

/// <summary>
/// Хранилище состояний саг с оптимистичной блокировкой по версии (док 17, §2).
/// Реализации: in-memory для тестов, EF Core для продакшена.
/// </summary>
public interface ISagaStore
{
    /// <summary>
    /// Загружает состояние по ключу корреляции. <c>null</c> — инстанса ещё нет.
    /// Возвращает версию для оптимистичной блокировки.
    /// </summary>
    ValueTask<(TState state, int version)?> LoadAsync<TState>(
        Type sagaType,
        string correlationKey,
        CancellationToken ct = default) where TState : SagaState;

    /// <summary>
    /// Сохраняет состояние. <paramref name="correlationKey"/> — тот же ключ, что в
    /// <see cref="LoadAsync{TState}"/>; <paramref name="expectedVersion"/> — версия, от которой
    /// исходили; при расхождении бросает <see cref="SagaConcurrencyException"/>.
    /// </summary>
    ValueTask SaveAsync<TState>(
        Type sagaType,
        string correlationKey,
        TState state,
        int expectedVersion,
        CancellationToken ct = default) where TState : SagaState;

    /// <summary>Удаляет состояние завершённой саги.</summary>
    ValueTask CompleteAsync(Type sagaType, Guid instanceId, CancellationToken ct = default);
}

/// <summary>Конфликт оптимистичной блокировки: инстанс уже обновлён другим обработчиком.</summary>
public sealed class SagaConcurrencyException(Guid instanceId, int expectedVersion)
    : Exception($"Saga '{instanceId}' уже обновлён: ожидалась версия {expectedVersion}.")
{
    public Guid InstanceId { get; } = instanceId;

    public int ExpectedVersion { get; } = expectedVersion;
}

/// <summary>In-memory хранилище — для тестов и монолита (док 17, §2).</summary>
public sealed class InMemorySagaStore : ISagaStore
{
    private readonly ConcurrentDictionary<(Type SagaType, string Key), Entry> _rows = [];

    private sealed class Entry(object state, int version, Guid instanceId)
    {
        public object State { get; set; } = state;

        public int Version { get; set; } = version;

        public Guid InstanceId { get; } = instanceId;
    }

    public ValueTask<(TState state, int version)?> LoadAsync<TState>(
        Type sagaType,
        string correlationKey,
        CancellationToken ct = default) where TState : SagaState
    {
        if (_rows.TryGetValue((sagaType, correlationKey), out var entry))
            return ValueTask.FromResult<(TState state, int version)?>(((TState)entry.State, entry.Version));

        return ValueTask.FromResult<(TState state, int version)?>(null);
    }

    public ValueTask SaveAsync<TState>(
        Type sagaType,
        string correlationKey,
        TState state,
        int expectedVersion,
        CancellationToken ct = default) where TState : SagaState
    {
        var key = (sagaType, correlationKey);
        var instanceId = state.Id;

        // Новый инстанс: только если ещё не существует.
        if (expectedVersion == 0)
        {
            var added = _rows.TryAdd(key, new Entry(state, 1, instanceId));
            if (!added)
                throw new SagaConcurrencyException(instanceId, 0);

            return ValueTask.CompletedTask;
        }

        // Обновление: CAS-петля по версии.
        while (true)
        {
            if (!_rows.TryGetValue(key, out var existing))
                throw new SagaConcurrencyException(instanceId, expectedVersion);

            if (existing.Version != expectedVersion)
                throw new SagaConcurrencyException(instanceId, expectedVersion);

            var replacement = new Entry(state, existing.Version + 1, instanceId);
            if (_rows.TryUpdate(key, replacement, existing))
                return ValueTask.CompletedTask;
        }
    }

    public ValueTask CompleteAsync(Type sagaType, Guid instanceId, CancellationToken ct = default)
    {
        foreach (var key in _rows.Keys)
        {
            if (key.SagaType != sagaType)
                continue;

            if (_rows.TryGetValue(key, out var entry) && entry.InstanceId == instanceId)
                _rows.TryRemove(key, out _);
        }

        return ValueTask.CompletedTask;
    }
}
