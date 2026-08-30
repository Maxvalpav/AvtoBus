# ADR-0002: Lifetime IBus и граница Unit of Work

- Статус: Proposed
- Дата: 2026
- Область: DI, Outbox, транзакции

## Контекст

`IBus` нужен как singleton-friendly facade, но EF Core Outbox зависит от scoped `DbContext`. Прямая инъекция scoped `IOutbox` в singleton `DefaultBus` создаёт captive dependency. Ambient `ConsumeContext` не решает отправку из HTTP endpoint, где входящего сообщения нет.

## Решение

Разделить две роли:

1. `IBus` - singleton facade для немедленной отправки вне бизнес-транзакции.
2. `IMessageSession` - scoped буфер отправки, связанный с текущим Unit of Work.

```csharp
public interface IMessageSession
{
    ValueTask Send<T>(T command, SendOptions? options = null, CancellationToken ct = default);
    ValueTask Publish<T>(T @event, PublishOptions? options = null, CancellationToken ct = default);
}
```

Правила:

- Handler и HTTP endpoint получают `IMessageSession`, если сообщения должны быть атомарны с изменениями БД.
- `IBus` не пытается автоматически угадать наличие транзакции.
- EF provider записывает buffered messages в outbox в той же транзакции, что и бизнес-данные.
- После rollback буфер не публикуется.
- После commit relay доставляет outbox независимо от процесса-инициатора.
- Каскадный return handler-а добавляется в текущий `IMessageSession`.

## Transaction boundary

```text
Begin transaction
  Load/change domain data
  Buffer outgoing messages in IMessageSession
  Save business data
  Insert outbox rows
Commit transaction
Signal relay
```

`SaveChangesInterceptor` может материализовать буфер, но не должен самостоятельно коммитить транзакцию.

## Failure matrix

| Сбой | Ожидаемый результат |
|---|---|
| Exception до `SaveChanges` | Нет business data, нет outbox |
| Exception после business SQL, до commit | Rollback business data и outbox |
| Crash после commit, до signal relay | Polling relay подбирает outbox |
| Broker недоступен | Outbox остаётся pending, применяется backoff |
| Повтор relay после ambiguous confirm | Возможен дубль, consumer Inbox его подавляет |

## Последствия

- API становится честным: atomic send требует `IMessageSession`.
- `IBus.Publish` из HTTP без session остаётся допустимым, но явно не атомарным с БД.
- Не нужен `HasUnitOfWork` в `ConsumeContext`.
- Для нескольких `DbContext` требуется явный выбор owner context; distributed transaction не обещается.

## Проверка решения

- DI test с `ValidateScopes=true`.
- PostgreSQL integration tests для всех строк failure matrix.
- Тест каскадного handler return в той же транзакции.
- Тест HTTP endpoint с scoped session без входящего `ConsumeContext`.