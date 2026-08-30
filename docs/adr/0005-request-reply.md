# ADR-0005: Request/Reply routing и lifecycle

- Статус: Proposed
- Дата: 2026
- Область: messaging patterns

## Контекст

Ранний эскиз создаёт `TaskCompletionSource`, но не определяет reply endpoint, routing, late reply и cleanup. Request/Reply поверх broker-а требует отдельного lifecycle и не должен маскировать синхронную связанность.

## Решение

### API

```csharp
ValueTask<TReply> Request<TRequest, TReply>(
    TRequest request,
    RequestOptions? options = null,
    CancellationToken ct = default);
```

`RequestOptions` требует timeout. Default timeout допустим, но всегда записывается в envelope deadline.

### Reply endpoint

MVP использует один shared reply endpoint на service instance:

```text
avtobus.reply.{service}.{instance-id}
```

Request envelope содержит:

- `reply_to` - адрес endpoint-а;
- `request_id` - id запроса;
- `correlation_id`;
- `deadline_utc`.

Reply envelope содержит `request_id` исходного запроса. `ReplyPump` читает endpoint и завершает соответствующий waiter.

### Waiter lifecycle

```text
Register waiter
  -> Publish request
  -> Reply received -> Complete -> Remove
  -> Timeout -> Fail -> Remove
  -> Caller cancellation -> Cancel -> Remove
  -> Host shutdown -> Cancel all -> Drain reply endpoint
```

Регистрация waiter-а происходит до publish, чтобы быстрый reply не потерялся.

### Late replies

Reply после timeout/cancellation:

- не requeue;
- записывается в metric `avtobus.reply.late`;
- payload не логируется без redaction policy;
- при audit mode сохраняется metadata без тела.

### Ограничения

- Request/Reply не заменяет HTTP/gRPC для синхронного RPC.
- Один request имеет ровно один логический reply.
- Streaming replies и scatter-gather не входят в MVP.
- Timeout не отменяет уже выполняющийся remote handler автоматически.

## Последствия

- Нужен отдельный `ReplyPump` hosted service.
- Temporary/shared queue semantics реализуются каждым transport provider.
- InMemory provider использует тот же public lifecycle, даже если оптимизирует доставку.

## Проверка решения

- Reply приходит до завершения `Publish`: waiter уже зарегистрирован.
- Timeout удаляет waiter и не течёт памятью.
- Caller cancellation удаляет waiter.
- Late reply не requeue и увеличивает metric.
- Host shutdown завершает все waiters `OperationCanceledException`.
- Две replicas не завершают waiter друг друга благодаря instance-specific reply endpoint.