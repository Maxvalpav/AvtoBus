# Kafka + Postgres — два реальных сервиса на AvtoBus

Два независимых сервиса (`ServiceA` orders, `ServiceB` inventory) общаются через **Kafka (Redpanda)** и хранят outbox/inbox/saga в **Postgres** транзакционно.

## Архитектура
```
ServiceA (8080) --SubmitOrder--> Kafka orders.commands --┐
  Postgres avtobus_a (outbox)                           │
                                                        v
                                                  ServiceB (8081)
                                                  Postgres avtobus_b (inventory -out-of-stock check)
                                                        │
ServiceA <--OrderConfirmed-- Kafka orders.events --------┘
  Postgres avtobus_a (inbox dedup)
```
- `SubmitOrder:ICommand,IPartitionedMessage` (PartitionKey=OrderId) → `RouteCommand` `kafka/orders.commands` (точно один владелец, ordered по партиции)
- `OrderConfirmed/OrderFailed:IEvent` → `RouteEvent` `kafka/orders.events` (подписчики, consumer group)
- Outbox: `ServiceA` `bus.SendAsync` пишет `avtobus.outbox_messages` в той же транзакции что и `app.orders`; `OutboxDispatcherService` `ClaimBatch FOR UPDATE SKIP LOCKED` → `KafkaTransport.SendAsync` → `MarkDispatched`
- Inbox: `ServiceA` `IsDuplicate(MessageId,ConsumerId)` → `MarkConsumed` в той же транзакции что и `projection`
- Kafka: `Redpanda` (Kafka-совместим), `BootstrapServers=redpanda:9092`, `PartitionCount=12`, `Compression=lz4`, `EnableIdempotence=true`
- Postgres: `avtobus` схема (11 таблиц) + `app` схема (orders/inventory)

## Запуск
```bash
docker compose -f examples/KafkaPgTwoServices/docker-compose.yml up --build -d
# миграции применятся автоматически (Database.MigrateAsync)
curl -X POST "http://localhost:8080/orders?sku=SKU-1&qty=2"
# -> {"orderId":"...","status":"Submitted via outbox -> Kafka orders.commands"}
curl http://localhost:8080/confirmed
# -> [{"orderId":"...","sku":"SKU-1"}]  (ServiceA получил OrderConfirmed от ServiceB)
# dashboard
open http://localhost:8080/avtobus   # ServiceA outbox/inbox/DLQ/metrics
open http://localhost:8081/avtobus   # ServiceB

# без Docker — InMemory fallback (Kafka/PG недоступны, используется InMemoryTransport+InMemory stores, та же модель)
dotnet run --project examples/KafkaPgTwoServices/ServiceA --urls http://localhost:5000
dotnet run --project examples/KafkaPgTwoServices/ServiceB --urls http://localhost:5001

# тесты двух сервисов (shared InMemoryTransport, static queues)
dotnet test --filter TwoServices
```

## Проверка outbox/inbox
```sql
-- в postgres
SELECT id, state, destination, transport, attempt_count, next_attempt_at FROM avtobus.outbox_messages ORDER BY created_at DESC LIMIT 5;
SELECT message_id, consumer_id, message_type FROM avtobus.inbox_messages;
SELECT id, reason, endpoint, failed_at FROM avtobus.dead_letters;
```

## Примечание
`AvtoBus.Transport.Kafka` сейчас channel-backed для dev/test (без внешнего брокера) — в проде замени на `Confluent.Kafka` producer/consumer (см. `KafkaTransport.cs` коммент `// In production: produce to Kafka topic...`). Транспорт-нейтральность сохранена: замена `RouteCommand("kafka",...)` на `("rabbitmq",...)` или `("inmemory",...)` без изменения handler-ов.
