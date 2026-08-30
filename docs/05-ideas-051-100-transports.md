# 💡 Идеи 51–100: Транспорты и брокеры

### 51. Единый контракт транспорта из 2 методов (Watermill)
`SendAsync` + `ReceiveAsync(IAsyncEnumerable)` — новый транспорт пишется за день.

### 52. In-Memory транспорт с полной семантикой брокера
Очереди, топики, ретраи, DLQ, задержки — для тестов и модульных монолитов. Основа: `System.Threading.Channels`.

### 53. RabbitMQ: quorum queues по умолчанию
```csharp
bus.UseRabbitMq(r => r.UseQuorumQueues(deliveryLimit: 6)); // встроенный retry-счётчик брокера
```

### 54. RabbitMQ Streams для событийных лент
`bus.Topic<OrderPlaced>().AsStream(retention: 7.Days())` — реплей истории новым подписчиком.

### 55. Автотопология из типов (MassTransit)
Exchange на тип события + bind иерархии наследования; создаётся идемпотентно при старте.

### 56. Декларативная миграция топологии
`avtobus topology diff/apply` — как EF migrations, но для exchanges/queues/bindings.

### 57. Kafka: exactly-once через транзакции
```csharp
bus.UseKafka(k => k.ExactlyOnce()); // producer transactional.id + read_committed + sendOffsetsToTransaction
```

### 58. Kafka: ключ партиции из сообщения
```csharp
[PartitionKey] public string AccountId { get; init; } // атрибут → key продюсера
```

### 59. Kafka: пауза/резюм партиций при back-pressure
При заполнении внутреннего буфера консьюмер вызывает `Pause(partitions)` — нет OOM.

### 60. Kafka: параллелизм внутри партиции с сохранением порядка по ключу
Идея Confluent Parallel Consumer: очередь на ключ, оффсеты коммитятся «водяным знаком».

### 61. Azure Service Bus: sessions для строгого порядка
`bus.Consumer<T>().RequireSessions(m => m.OrderId)` — маппинг на ASB session id.

### 62. ASB: авто-продление lock-а долгих сообщений
RenewLock в фоне пока хендлер работает; настройка `MaxAutoRenewDuration`.

### 63. NATS JetStream транспорт
Pull-consumers с batch fetch — идеальный back-pressure; subject-wildcard подписки.

### 64. NATS KV как distributed cache шины
Хранение saga-state / дедуп-ключей в JetStream KV — без внешней БД.

### 65. Redis Streams транспорт
`XADD/XREADGROUP/XAUTOCLAIM` — переподхват зависших сообщений упавших консьюмеров.

### 66. SQL-транспорт (очереди в PostgreSQL/SQL Server)
```sql
SELECT ... FROM queue_messages
WHERE visible_at <= now() ORDER BY id
FOR UPDATE SKIP LOCKED LIMIT 10; -- конкурентные читатели без блокировок
```
Для команд внутри одного кластера БД — транзакционность бесплатно.

### 67. PostgreSQL LISTEN/NOTIFY как push-триггер SQL-транспорта
Поллинг с длинным интервалом + мгновенное пробуждение по NOTIFY.

### 68. Amazon SQS/SNS транспорт с fan-out
SNS topic → SQS queues; FIFO-очереди для ordered; батч-отправка по 10.

### 69. Google Pub/Sub транспорт
Ordering keys, exactly-once delivery ack-ids, dead letter topics.

### 70. MQTT-транспорт для IoT-сценариев
QoS 0/1/2 маппится на at-most/at-least/exactly-once семантики шины.

### 71. gRPC-транспорт «точка-точка»
Для низколатентных команд между двумя сервисами без брокера; bidi-stream как канал.

### 72. WebSocket/SignalR мост
`bus.BridgeToSignalR<OrderStatusChanged>(hub => hub.Group(msg.CustomerId))` — события сразу в браузер.

### 73. Мульти-транспорт в одном приложении
Команды — RabbitMQ, аналитика — Kafka, уведомления — Redis:
```csharp
bus.Routes(r =>
{
    r.Events().FromNamespace("Analytics").Via("kafka");
    r.Command<SendPush>().Via("redis");
});
```

### 74. Транспорт-мост (bridge) между брокерами
Консьюмим из Kafka, публикуем в RabbitMQ — миграция брокера без даунтайма.

### 75. Shadow-транспорт для канареечных сравнений
Дублируем трафик в новый брокер, сравниваем результаты, но ack только по основному.

### 76. Failover-транспорт
Список приоритетных брокеров; при недоступности — автоматический переход + буферизация в локальный durable-спул.

### 77. Локальный durable-спул при отказе брокера
Сообщения пишутся в SQLite/файл, отправляются при восстановлении соединения (как NServiceBus critical-time buffer).

### 78. Connection resiliency: один канал переподключения
Единая политика reconnect с экспоненциальным джиттер-бэкоффом для всех транспортов (Polly v8).

### 79. Health транспорта → readiness probe
Брокер недоступен дольше N секунд → readiness false → Kubernetes убирает под из балансировки.

### 80. Ленивые подключения
Соединение с брокером открывается при первом использовании; `WarmUp()` — для прогрева при старте.

### 81. Мультиплексирование каналов RabbitMQ
Пул каналов per-CPU с round-robin; publisher confirms батчами (см. идею 360).

### 82. Publisher confirms с корреляцией
`await` завершается только после confirm брокера; nack → ретрай в другой канал.

### 83. Чанкинг больших сообщений (Silverback)
Сообщение > 1 МБ режется на чанки с заголовками `chunk-index/chunk-count`, собирается на приёме.

### 84. Claim Check паттерн
```csharp
bus.UseClaimCheck(s3, threshold: 256.Kilobytes());
// тело → S3/Azure Blob, в брокер идёт только ссылка + hash
```

### 85. Компрессия per-message
`Content-Encoding: gzip|zstd|br` заголовок; auto при body > порога; zstd-словарь под контракты.

### 86. Native delayed exchange / scheduled enqueue
Делегируем задержку брокеру, где умеет (RabbitMQ delayed plugin, ASB ScheduledEnqueueTime, SQS DelaySeconds); фолбэк — retry-очереди с TTL.

### 87. Топология retry-очередей с TTL-бэкоффом
`orders.retry.5s → orders.retry.30s → orders.retry.5m` — dead-letter-цепочка без плагинов.

### 88. Consumer groups поверх RabbitMQ
Эмуляция Kafka consumer-groups: single-active-consumer + авторебаланс очередей-партиций.

### 89. Единая модель партиций для всех транспортов
`PartitionKey` в Envelope: Kafka — key, ASB — session, RabbitMQ — consistent-hash exchange, InMemory — канал по hash.

### 90. Автосоздание DLQ и парковочных очередей
`{queue}.error` (транзиентные после ретраев) и `{queue}.poison` (десериализация/контракт) — разные судьбы.

### 91. Read-only реплей из DLQ
`avtobus dlq replay orders.error --filter 'type=PlaceOrder' --rate 10/s` — с ограничением скорости.

### 92. Приоритет чтения: сначала retry, потом основная
Настраиваемые весовые пропорции чтения из нескольких очередей одним консьюмером.

### 93. Транспортные хинты в контракте
```csharp
[Message(Durable = false, Priority = 9, Ttl = "00:05:00")]
public record LiveTick(...); // недолговечные тики — non-persistent delivery
```

### 94. Наблюдение глубины очередей встроено
Периодический опрос management API/admin client → метрика `avtobus.queue.depth` → автоскейлинг KEDA.

### 95. KEDA-совместимые скейлеры
Готовые ScaledObject-манифесты генерятся CLI: `avtobus keda generate --queue orders`.

### 96. Мягкая деградация: sampling при перегрузке
Для не-критичных подписок настраиваемый drop-режим: обрабатывать каждый N-й при глубине > X (только для `IEvent` с `[Lossy]`).

### 97. Транспортные интеграционные тесты через Testcontainers
`AvtoBus.Testing.Containers`: `await RabbitFixture.Start()` — контейнер за 2 строки, общий для сьюта.

### 98. Спецификация соответствия транспорта (conformance kit)
Набор из ~80 тестов семантики (порядок, редоставка, TTL, DLQ), который обязан пройти каждый транспорт — как Jepsen-lite.

### 99. Экзотика как community-транспорты: ZeroMQ, Pulsar, EventStoreDB, Chronicle-подобный файловый лог
Единый contract kit (идея 98) делает их дешёвыми в поддержке.

### 100. Файловый транспорт append-only log (Chronicle Queue-style)
Memory-mapped файлы, микросекундные латентности, для одного хоста / IPC между процессами:
```csharp
bus.UseFileLog("/var/avtobus/log", segmentSize: 256.Megabytes());
```
