# 💡 Идеи 1–50: Ядро шины и дизайн API

### 1. Method-handlers без интерфейсов (Wolverine)
Любой `public static`/instance метод `Handle`/`Consume` — хендлер. Обнаружение на компиляции.
```csharp
public static OrderPlaced Handle(PlaceOrder cmd, IOrderRepo repo) => repo.Place(cmd);
```

### 2. Каскадные сообщения через возврат
Возврат из хендлера публикуется автоматически. Кортеж = несколько сообщений. `null` = ничего.

### 3. `OutgoingMessages` — динамический билдер каскадов
```csharp
var o = new OutgoingMessages();
o.Send(cmd); o.Publish(evt); o.Schedule(msg, 5.Minutes()); o.RespondTo(ctx, reply);
```

### 4. Source Generator вместо рефлексии
Диспетчеры, роутинг-таблица и JSON-контексты генерируются на компиляции. 100% Native AOT.

### 5. Компайл-тайм диагностики шины
`AVB001: команда PlaceOrder никем не обрабатывается` — ошибка ещё до запуска.

### 6. Три уровня API: интерфейс / метод / лямбда
Постепенное усложнение, миграция с MassTransit без переучивания.

### 7. `IBusMiddleware` — единый механизм расширения
```csharp
public sealed class StopwatchMiddleware : IBusMiddleware
{
    public async ValueTask InvokeAsync(ConsumeContext ctx, BusDelegate next)
    {
        var sw = Stopwatch.StartNew();
        try { await next(ctx); }
        finally { BusMetrics.ConsumeDuration.Record(sw.ElapsedMilliseconds); }
    }
}
```

### 8. Условные middleware (`UseWhen`)
```csharp
p.UseWhen(ctx => ctx.Envelope.TenantId is not null, b => b.Use<TenantBillingMiddleware>());
```

### 9. Полиморфная диспетчеризация (Rebus)
Подписка на `IOrderEvent` ловит все наследники. Реализуется таблицей type-hierarchy в codegen.

### 10. Разделение Command / Event на уровне типов
```csharp
public interface ICommand;  // ровно один получатель, Send
public interface IEvent;    // 0..N получателей, Publish
```
`bus.Publish(command)` — ошибка компиляции через анализатор (NServiceBus best practice).

### 11. Envelope с `ReadOnlyMemory<byte>` — zero-copy
Тело не копируется от транспорта до десериализатора; pooling через `MemoryPool<byte>`.

### 12. CorrelationId/CausationId автоматически
Каждое каскадное сообщение получает `CausationId = parentMessageId`, `CorrelationId` наследуется. Даёт полное дерево причинности.

### 13. `ConsumeContext.Items` — как HttpContext.Items
Обмен данными между middleware в рамках одной обработки.

### 14. Scoped DI на сообщение
Каждая обработка = новый `IServiceScope`; `IHttpContextAccessor`-аналог: `IBusContextAccessor`.

### 15. In-process локальные очереди (Wolverine local queues)
```csharp
bus.LocalQueue("thumbnails").MaxParallelism(4).UseDurableInbox();
await bus.EnqueueLocal(new ResizeImage(id)); // без брокера, через Channel<T>
```

### 16. Request/Response с типизированными ответами
`Request<TReq,TReply>` через временные reply-очереди или NATS-style inbox.

### 17. Множественные ответы: `RequestStream`
```csharp
await foreach (var chunk in bus.RequestStream<ExportOrders, OrderChunk>(req, ct)) { ... }
```
Идея из gRPC server-streaming поверх шины.

### 18. Fluent-опции отправки
```csharp
await bus.Publish(evt, o => o.WithDelay(10.Minutes()).WithHeader("source", "api").WithPriority(9));
```

### 19. `IMessageBatch<T>` — батч-хендлер (Broadway)
```csharp
public static Task Handle(IMessageBatch<PriceChanged> batch, IBulkWriter db)
    => db.BulkUpsert(batch.Messages); // один INSERT на 500 событий
```

### 20. Батчинг с настройкой size/timeout/partition
```csharp
bus.Consumer<PriceChanged>().Batch(size: 500, timeout: 200.Milliseconds(), partitionBy: m => m.Exchange);
```

### 21. Back-pressure по demand (Broadway/Reactive Streams)
Консьюмер запрашивает у транспорта ровно столько, сколько может обработать; никаких переполненных буферов.

### 22. Конвейер стадий: Producer → Processor → Batcher
Мини-Broadway: декларативная топология внутри одного процесса.

### 23. `IAsyncEnumerable<T>` как источник сообщений
Любой асинхронный поток можно подключить как транспорт:
```csharp
bus.UseCustomSource("file-import", ReadCsvLines("orders.csv").Select(l => new ImportRow(l)));
```

### 24. Приоритетные очереди на уровне API
`[Priority(High)]` на сообщении → RabbitMQ priority / отдельная очередь в Kafka.

### 25. Партиционированная обработка с сохранением порядка
```csharp
bus.Consumer<AccountEvent>().OrderedBy(m => m.AccountId, partitions: 32);
```
Внутри — 32 канала-актора, сообщения одного аккаунта строго последовательны (идея Orleans/Akka).

### 26. Sticky-хендлеры per-key (partition actors)
Состояние хендлера живёт между сообщениями одного ключа (mini-actor), выгружается по idle-таймауту.

### 27. Виртуальные топики для модульного монолита (Spring Modulith)
В монолите события идут in-memory, при выносе модуля — конфиг меняет транспорт, код не трогаем.

### 28. `Deferred<T>` — ленивые зависимости хендлера
Инжекция `Deferred<IExpensiveService>` — резолв только при фактическом использовании.

### 29. Уникальные сообщения (Oban unique jobs)
```csharp
await bus.Send(new RecalcRating(userId), o => o.Unique(by: userId, within: 1.Hours()));
```
Повторный enqueue в окне игнорируется (constraint в inbox-таблице).

### 30. Debounce/Throttle сообщений
```csharp
bus.Consumer<SearchIndexUpdate>().Debounce(m => m.DocumentId, 5.Seconds());
```
Сливает шквал апдейтов одного документа в один.

### 31. Message flows: родитель ждёт детей (BullMQ flows)
```csharp
var flow = bus.Flow()
    .Child(new RenderPage(1)).Child(new RenderPage(2))
    .Parent(new MergePdf(docId));
await flow.Dispatch(); // MergePdf выполнится после всех детей
```

### 32. Canvas-композиция: chain / group / chord (Celery)
```csharp
await bus.Chain(new Extract(url)).Then<Transform>().Then<Load>().Dispatch();
await bus.Group(urls.Select(u => new Crawl(u))).Chord(new BuildIndex()).Dispatch();
```

### 33. `[SubscribeAll("orders.*")]` — wildcard-подписки (NATS)
Иерархичные subject-ы с `*` и `>` для всех транспортов (эмуляция там, где нативно нет).

### 34. Явный контроль ack: `ctx.Ack()/Nack()/Requeue()`
По умолчанию auto-ack после хендлера, но можно взять управление (manual mode).

### 35. Graceful shutdown с drain
`IHostApplicationLifetime` → стоп приёма, дожидание in-flight (таймаут), возврат недоделанного в очередь.

### 36. `PauseToken` — пауза консьюмеров на лету
```csharp
await busControl.Pause("orders-consumer");  // из дашборда/CLI, без рестарта
```

### 37. Динамическое масштабирование concurrency
`busControl.SetConcurrency("orders", 16)` в рантайме; авторегулировка по глубине очереди.

### 38. Наследование контекста: `Baggage`
W3C Baggage сквозь всю цепочку сообщений: `ctx.Baggage["campaign"]`.

### 39. Обработчик «catch-all» `IConsumer<UnknownMessage>`
Сообщения без зарегистрированного типа не теряются — попадают в специальный хендлер/очередь.

### 40. `IStartupTask` шины
Декларативные преднастройки: создание топологии, прогрев кэшей, миграции outbox-таблиц до старта консьюмеров.

### 41. Профили окружений
```csharp
bus.Profile("Development", d => d.UseInMemory().DisableRetries());
bus.Profile("Production", p => p.UseRabbitMq(...).UseOutbox<AppDb>());
```

### 42. `AddAvtoBusClient` — лёгкий клиент без консьюмеров
Для API-гейтвеев: только Send/Publish, ноль фоновых сервисов.

### 43. Строготипизированные имена очередей
`QueueName`, `TopicName` — value-типы с валидацией (длина, символы брокера) на компиляции через анализатор.

### 44. Идемпотентный `Handle` через атрибут
```csharp
[Idempotent(Key = nameof(PlaceOrder.OrderId), Window = "24h")]
public static Task Handle(PlaceOrder cmd, ...) { ... }
```

### 45. Fire-and-forget с локальным подтверждением
`bus.PublishFast(evt)` — в Channel и сразу вернуться; для метрик/логов, где потеря допустима.

### 46. Отмена сообщений: `CancelScheduled`
```csharp
var token = await bus.Schedule(new SendReminder(id), at);
await bus.CancelScheduled(token); // пользователь уже оплатил
```

### 47. Дедлайны обработки: `Envelope.TimeToLive`
Протухшие сообщения не обрабатываются, а идут в expired-очередь с метрикой.

### 48. Reply-каналы без брокера для локальных вызовов
Если получатель в том же процессе, Request/Response идёт напрямую через `TaskCompletionSource` — микросекунды.

### 49. `IBusProbe` — health-модель шины
`/health` показывает: транспорт подключён, лаг консьюмеров, размер outbox, circuit breaker-ы.

### 50. Единый `Result`-контракт ошибок хендлера
```csharp
public static Result<OrderPlaced> Handle(PlaceOrder cmd) =>
    cmd.Items.Count == 0
        ? Result.Reject("empty_order")        // без ретраев → discard/DLQ
        : Result.Ok(new OrderPlaced(...));    // успех + каскад
```
Различие «бизнес-отказ» vs «транзиентная ошибка» задаёт стратегию ретраев.
