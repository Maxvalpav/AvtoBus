# TwoServices — два сервиса общаются разными фичами

## Архитектура
- AvtoBus.TwoServices.Contracts — общие контракты (OrderCreated, ReserveInventory, CheckStock...)
- AvtoBus.TwoServices.Orders (порт 5001) — фичи: Publish/Send/Schedule/Request (Kafka/Rabbit/InMemory)
- AvtoBus.TwoServices.Inventory (порт 5002) — фичи: Consume/Retry/Inbox dedup/Respond

## Транспорт — выбор по конфигу (приоритет: Kafka > Rabbit > InMemory)
`
ConnectionStrings:Kafka = "localhost:9092" -> UseKafka (ExactlyOnce, Lz4, Earliest)
ConnectionStrings:Rabbit = "amqp://..."     -> UseRabbitMq
(пусто)                                    -> UseInMemory (без Docker)
`

## Запуск (без Docker — InMemory)
`
dotnet run --project samples/AvtoBus.TwoServices.Orders --urls http://localhost:5001 &
dotnet run --project samples/AvtoBus.TwoServices.Inventory --urls http://localhost:5002 &
curl -X POST http://localhost:5001/orders -H "Content-Type: application/json" -d '{"customerId":"c1","sku":"sku1","quantity":2,"amount":100}'
curl http://localhost:5001/stock/sku1
`

## Запуск с Kafka (требует docker-compose.dev.yml)
`
docker compose -f build/docker-compose.dev.yml up -d kafka
# appsettings.Development.json уже ставит Kafka=localhost:9092
dotnet run --project samples/AvtoBus.TwoServices.Orders --urls http://localhost:5001
dotnet run --project samples/AvtoBus.TwoServices.Inventory --urls http://localhost:5002
# теперь сообщения идут через Kafka topic (6 partitions, Lz4, ExactlyOnce опционально)
`

## Фичи по сервисам
Orders: Publish OrderCreated (fan-out), Send ReserveInventory (queue), Request CheckStock->StockResult, Schedule ShippingScheduled, Recoverability Immediate+Delayed+Backoff.Exponential
Inventory: Consume OrderCreated, ReserveInventory с retry+DLQ, Respond CheckStock, ShippingScheduled, Inbox deduplication

## Кафка-специфика
- KafkaOptions.BootstrapServers, ConsumerGroup (orders-group/inventory-group), AutoOffsetReset Earliest, ExactlyOnce (transactional.id), Acks All, Compression Lz4
