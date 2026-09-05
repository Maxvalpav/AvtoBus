# Гарантии доставки — честно

Базовое правило AvtoBus: **at-least-once доставка + effectively-once обработка**
через inbox-дедуп или идемпотентный хендлер. Дубль — норма, а не баг.

## По транспортам

| Транспорт | Доставка | Порядок | Delayed | DLQ |
|---|---|---|---|---|
| InMemory | at-least-once в процессе | FIFO в очереди | ✅ | `.error`/`.poison`/`.expired` |
| RabbitMQ | at-least-once, publisher confirms | в очереди | ✅ | ✅ |
| SQL (PostgreSQL) | at-least-once, SKIP LOCKED | выборка по Id | ✅ | ✅ |
| Kafka | at-least-once (idempotent producer) | внутри партиции | ✅ через scheduling-фолбэк | ✅ |
| NATS JetStream | at-least-once | per subject | ✅ через scheduling-фолбэк | ✅ |
| Redis Streams | at-least-once, consumer groups | per stream | ✅ через scheduling-фолбэк | ✅ |
| Azure Service Bus | at-least-once (PeekLock) | сессии | scheduled enqueue | ✅ |

## Отложенная доставка без нативной поддержки (04 §1.1)

`DeferAsync` и ретрай с бэкоффом работают одинаково везде: на InMemory, RabbitMQ,
SQL и ASB — нативно (`ISupportsDelayedDelivery`), на Kafka/NATS/Redis — через
фолбэк в `IScheduleStore` (нужен `UseScheduling`, in-memory или EF-стор):
ретрай персистится со сроком и возвращается в транспорт штатным циклом шедулера.

Адрес ретрая выбирает транспорт (`IDelayedRetryMapper`): Kafka/Redis — в тот же
топик/стрим (группа перечитает), NATS — в исходную подписку (синтетический
`Queue("{dest}:{group}")` не читает никто). Следствия: NATS-топики дают fan-out
копию всем группам — включайте inbox-дедуп (`UseRedisInbox`/`UseInMemoryInbox`);
без `UseScheduling` задержка громко отбрасывается (warning) с немедленным requeue.

Conformance-сьюты в CI: все, кроме Azure Service Bus (для него нужен живой Azure,
прогон ручной).

## Про «exactly-once» (E-16)

Kafka-транзакции дают exactly-once **только для цепочки consume→produce внутри
Kafka**. Хендлер с внешними побочными эффектами (БД, HTTP) ровно-один-раз
не выполняется — для этого нужны inbox-дедуп и идемпотентность на вашей стороне.
Формулировка «exactly-once опционально (транзакции)» без этого уточнения —
некорректна; корректно: «транзакции для Kafka→Kafka; внешние эффекты требуют
inbox-дедупа/идемпотентности».

## Outbox поверх любого транспорта

At-least-once + FIFO per `PartitionKey` (партиционные лизы relay). Сообщения без
ключа порядковых гарантий между собой не имеют. Зависший relay (crash между claim
и publish) — пере-claim по `StaleClaim`, дубль возможен и гасится inbox-дедупом.

## Подписи

Исходящие — v3 (подписанная метка времени, окно 5 мин + допуск на часы 1 мин).
Входящие принимаются v2/v3. Подробности и политика — в [compatibility](compatibility.md).
