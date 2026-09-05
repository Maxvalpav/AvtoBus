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
  Для Kafka / NATS / Redis — через `AvtoBus.Scheduling` (единый `DelayedDeliveryStore`
  для всех транспортов — в дорожной карте).
- **Inbox-дедуп** сегодня — через EF Core (`UseOutbox<TDbContext>`). Хранилища без
  реляционной БД (Redis / in-memory) — в дорожной карте.
- **Порядок**: строгий глобальный порядок не даёт никто; гарантии — в пределах
  очереди / партиции / subject / stream (см. [guarantees](guarantees.md)).
- Сомневаетесь — стартуйте с InMemory + outbox-контрактов: смена транспорта позже
  не трогает хендлеры.
