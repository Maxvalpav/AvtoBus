# Гарантии доставки — честно

Базовое правило AvtoBus: **at-least-once доставка + effectively-once обработка**
через inbox-дедуп или идемпотентный хендлер. Дубль — норма, а не баг.

## По транспортам

| Транспорт | Доставка | Порядок | Delayed | DLQ |
|---|---|---|---|---|
| InMemory | at-least-once в процессе | FIFO в очереди | ✅ | `.error`/`.poison`/`.expired` |
| RabbitMQ | at-least-once, publisher confirms | в очереди | ✅ | ✅ |
| SQL (PostgreSQL) | at-least-once, SKIP LOCKED | выборка по Id | ✅ | ✅ |
| Kafka | at-least-once (idempotent producer) | внутри партиции | — (эмулируется через `AvtoBus.Scheduling`) | ✅ |
| NATS JetStream | at-least-once | per subject | — | ✅ |
| Redis Streams | at-least-once, consumer groups | per stream | — | ✅ |
| Azure Service Bus | at-least-once (PeekLock) | сессии | scheduled enqueue | ✅ |

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
