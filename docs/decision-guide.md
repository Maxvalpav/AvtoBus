# Какой транспорт выбрать

| Ваш случай | Берите | Почему |
|---|---|---|
| Модульный монолит без брокера | `AvtoBus` (InMemory внутри) | Ноль инфраструктуры, `bus.UseInMemory()` |
| Сервис с RabbitMQ и транзакционным outbox | `AvtoBus.RabbitMq` + `AvtoBus.Outbox.EfCore` | Quorum-очереди, confirms, DLQ, атомарность с БД |
| Поток событий, партиции по ключу | `AvtoBus.Kafka` | Порядок внутри партиции, back-pressure |
| Лёгкий стриминг без ZooKeeper/KRaft-зоопарка | `AvtoBus.Nats` | JetStream, queue groups, `MaxAckPending` |
| Очередь поверх уже имеющегося Redis | `AvtoBus.Redis` | Consumer groups, XAUTOCLAIM зависших |
| Очередь без брокера вообще (есть только PostgreSQL) | `AvtoBus.Sql` | `SKIP LOCKED` + `LISTEN/NOTIFY` |
| Вы в Azure и нужен managed-брокер | `AvtoBus.AzureServiceBus` | Сессии, scheduled enqueue, lock renew |

## Нюансы

- **Delayed-доставка** нативно: InMemory, RabbitMQ, SQL, ASB (scheduled).
  Для Kafka / NATS / Redis — через scheduling-фолбэк (`UseScheduling`):
  ретрай с задержкой и `DeferAsync` персистятся в стор и возвращаются в транспорт
  по сроку. NATS-топики дают fan-out копию всем группам — включайте inbox-дедуп.
- **Inbox-дедуп**: EF Core (`UseOutbox<TDbContext>`, транзакционно), либо без БД —
  `UseInMemoryInbox` (монолит) / `UseRedisInbox` (мульти-инстанс, `SET NX EX`).
- **Порядок**: строгий глобальный порядок не даёт никто; гарантии — в пределах
  очереди / партиции / subject / stream (см. [guarantees](guarantees.md)).
- **Интероп**: `bus.UseCloudEvents("my-service")` — исходящие конверты несут
  ce-атрибуты CloudEvents 1.0 (`ce-id`, `ce-type`, `ce-source`, `ce-time`,
  `traceparent`) поверх собственных полей: читаются Knative, Dapr, Event Grid.
- Сомневаетесь — стартуйте с InMemory + outbox-контрактов: смена транспорта позже
  не трогает хендлеры.
