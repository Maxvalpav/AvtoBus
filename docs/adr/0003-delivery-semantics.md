# ADR-0003: Семантики доставки и термин exactly-once

- Статус: Proposed
- Дата: 2026
- Область: надёжность

## Контекст

Термин `exactly-once` часто используется без указания границы. Broker transaction не делает внешний HTTP side effect или запись в чужую БД exactly-once. Документация должна обещать только проверяемые свойства.

## Решение

AvtoBus использует следующие термины:

### At-most-once delivery

Сообщение может потеряться, но не доставляется повторно. Не является режимом по умолчанию.

### At-least-once delivery

Сообщение подтверждается после успешной обработки. При сбое возможна повторная доставка. Это режим по умолчанию.

### Effectively-once processing

Повторная доставка не повторяет транзакционные эффекты handler-а благодаря Inbox и одной локальной транзакции:

```text
insert inbox marker + business changes + outgoing outbox rows
```

Граница гарантии - одна поддерживаемая БД и один consumer identity.

### Broker exactly-once

Может предоставляться Kafka provider-ом для consume-transform-produce внутри одной Kafka transaction. Это не является end-to-end exactly-once.

## Запрещённое заявление

Документация и API не используют фразу `exactly-once` без одного из квалификаторов:

- `broker exactly-once`;
- `effectively-once database processing`;
- точной transaction boundary.

## Consumer identity

Inbox key:

```text
(message_id, consumer_id)
```

`consumer_id` является стабильным логическим именем endpoint-а, а не hostname/pod id. Иначе новая replica обработает сообщение повторно как новый consumer.

## Ack state machine

```text
Received
  -> Processing
  -> Committed -> Acked
  -> RetryScheduled
  -> DeadLettered
  -> Cancelled -> Requeued during shutdown
```

Ack выполняется только после commit local transaction. Ошибка ack после commit создаёт допустимую повторную доставку, которую подавляет Inbox.

## Последствия

- HTTP/webhook side effects требуют собственного idempotency key.
- Inbox retention задаёт временную границу дедупликации.
- Удаление Inbox раньше максимального broker redelivery window может повторить эффект.
- Пользователь обязан проектировать idempotent external integrations.

## Проверка решения

- Conformance test принудительно роняет process после commit, но до ack.
- Один `MessageId` доставляется двум replicas одного `consumer_id`; бизнес-эффект один.
- Два разных `consumer_id` законно обрабатывают одно событие независимо.
- Kafka provider публикует отдельный документ с точной EOS boundary.