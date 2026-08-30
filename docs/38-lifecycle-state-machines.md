# AvtoBus — Спецификация Стейт-Машин и Жизненных Циклов

> **Статус: Specification draft.** Формальное описание состояний, переходов и инвариантов ключевых сущностей AvtoBus.

---

## 1. Жизненный цикл Конверта Сообщения (Envelope Lifecycle)

```
                       ┌──────────────┐
                       │   Created    │
                       └──────┬───────┘
                              │ Publish / Send
                              ▼
                       ┌──────────────┐
                       │  In Outbox   │ (If transactional)
                       └──────┬───────┘
                              │ Relay Send
                              ▼
                       ┌──────────────┐
                       │  In Transport│ (Broker Queue)
                       └──────┬───────┘
                              │ Receive
                              ▼
                       ┌──────────────┐
                       │  Processing  │
                       └──────┬───────┘
             ┌────────────────┼────────────────┐
             │                │                │
      Handler Success   Transient Error   Fatal / Poison / Max Retries
             │                │                │
             ▼                ▼                ▼
      ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
      │   Acked     │  │ Retry Delay │  │ DeadLetter  │
      └─────────────┘  └──────┬──────┘  └─────────────┘
                              │
                              └─(Backoff expired)─> [ Processing ]
```

### Состояния:
1. **Created:** Конверт сформирован в памяти приложения, присвоен `MessageId` (UUID v7).
2. **In Outbox:** Записан в таблицу `avtobus_outbox` в одной транзакции с бизнес-данными.
3. **In Transport:** Находится в очереди/топике брокера (RabbitMQ/Kafka).
4. **Processing:** Извлечён консьюмером, создан `ConsumeContext`, выполняется middleware-пайплайн.
5. **Acked:** Хендлер завершился успешно. Отправлен `Ack` брокеру.
6. **Retry Delay:** Произошла транзиентная ошибка. Сообщение отправлено в `retry`-очередь с TTL.
7. **DeadLetter:** Исчерпаны все попытки или произошла невосстановимая ошибка (Validation/Poison). Сообщение перемещено в `.error` / `.poison` очередь.

---

## 2. Жизненный цикл Записи Outbox (Outbox Row State Machine)

```
[ New ] ──(DbContext.Save)──> [ Pending ] ──(Relay Claim)──> [ Claimed ]
                                                                 │
                                                    ┌────────────┴────────────┐
                                              Send Success               Send Fail
                                                    │                         │
                                                    ▼                         ▼
                                              [ Sent ]                  [ Retry Pending ]
                                                    │                         │
                                           (7 days cleanup)            (Release Claim)
                                                    │                         │
                                                    ▼                         ▼
                                             [ Deleted ]                [ Pending ]
```

### Инварианты Outbox:
- **`SentAt != NULL`** — сообщение гарантированно доставлено в брокер.
- **`ClaimedAt != NULL && ClaimedAt < Now - 2m`** — зависший claim (упал воркер). Автоматически сбрасывается для повторного claim другими воркерами.
- Порядок отправки внутри одного `PartitionKey` **строго монотоничен**.

---

## 3. Жизненный цикл Инстанса Саги (Saga Instance Lifecycle)

```
                    ┌──────────────┐
                    │  Not Exists  │
                    └──────┬───────┘
                           │ Message matching IStartedBy<T>
                           ▼
                    ┌──────────────┐
                    │    Active    │ <───┐
                    └──────┬───────┴─────┘ Message matching IHandle<T>
                           │
             ┌─────────────┼─────────────┐
             │             │             │
       MarkComplete()   SLA Timeout    Cancel / Abort
             │             │             │
             ▼             ▼             ▼
      ┌─────────────┐┌─────────────┐┌─────────────┐
      │  Completed  ││ SLA Violation││   Aborted   │
      └─────────────┘└─────────────┘└─────────────┘
```

### Правила переходов:
1. **Not Exists -> Active:** Приходит сообщение, помеченное `IStartedBy<T>` или `.StartsNew()`. Генерируется состояние саги.
2. **Active -> Active:** Сообщения коррелируют по `CorrelationKey`, обновляют состояние `TState` и увеличивают `Version` (optimistic locking).
3. **Active -> Completed:** Вызван `MarkComplete()`. Состояние помечается завершённым, плановые таймауты отменяются.
4. **Active -> Aborted:** Произошла фатальная ошибка или отмена. Запускается цепочка компенсаций в обратном порядке.

---

## 4. Жизненный цикл Волкера/Консьюмера (Consumer Lifetime)

```
 [ Stopped ] ──(StartAsync)──> [ Starting ] ──(Topology Declared)──> [ Running ]
                                                                         │
                                                                   (SIGTERM / Drain)
                                                                         │
                                                                         ▼
 [ Stopped ] <──(Connections Closed)── <──(In-Flight Drained) <── [ Draining ]
```

### Состояния:
1. **Starting:** Объявление топологии (queues, exchanges, bindings) в брокере.
2. **Running:** Подписка активна, приём сообщений с ограничением `PrefetchCount`.
3. **Draining:** Получен сигнал остановки (SIGTERM / K8s scale down). Приём **новых** сообщений прекращён, ожидается завершение обработок `in-flight` (в пределах `ShutdownTimeout`).
4. **Stopped:** Все соединения с брокером закрыты, ресурсы высвобождены.
