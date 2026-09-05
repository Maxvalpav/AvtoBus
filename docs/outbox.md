# Outbox и Inbox

## Зачем

Без outbox каскад `PublishAsync` уходит в транспорт ДО коммита бизнес-транзакции:
откат БД после отправки = «сообщение о том, чего не было». Outbox пишет сообщение
в ту же транзакцию, relay доставляет после коммита. Inbox гасит дубли на приёме.

## Подключение (EF Core)

```csharp
bus.UseOutbox<AppDbContext>(); // + UseProductionDefaults<AppDbContext>() для полного пресета
```

## Гарантии

- **At-least-once + FIFO per `PartitionKey`** при любом числе relay (партиционные
  лизы `avtobus_outbox_leases`, схема v3). Сообщения без ключа порядка между собой не имеют.
- **Crash-recovery**: relay, упавший между claim и publish, отдаёт строку обратно
  по `StaleClaim` — дубль возможен и гасится inbox-дедупом.
- **Inbox**: повторная доставка того же `MessageId` отбрасывается. Хендлеры всё равно
  обязаны быть идемпотентными — дедуп покрывает окно `InboxWindow` (дефолт 24ч).

## Схема БД

Текущая миграция outbox — **v3**. Relay читает v2/v3, пишет v3; downgrade ниже v2
не поддерживается. Политика — в [compatibility](compatibility.md).

## Без реляционной БД

Сегодня inbox/outbox требуют EF Core. Хранилища на Redis / in-memory —
в дорожной карте (аудит 04 §1.2).
