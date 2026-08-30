# 💡 Идеи 251–300: Event Sourcing и CQRS

### 251. Агрегат в стиле «Decider» (функциональный ES)
```csharp
public static class Order
{
    public static IEnumerable<object> Decide(OrderState s, PlaceOrder cmd) { /* правила */ yield return new OrderPlaced(...); }
    public static OrderState Evolve(OrderState s, object evt) => evt switch
    {
        OrderPlaced e => s with { Status = Placed, Items = e.Items },
        OrderPaid e   => s with { Status = Paid },
        _ => s
    };
}
```
Чистые функции = тривиальное тестирование; императивный `Aggregate<T>` — как альтернатива.

### 252. Аннотированный агрегат (Axon-style) как второй стиль
```csharp
public sealed class OrderAggregate : Aggregate
{
    [CommandHandler] public void Handle(PlaceOrder cmd) => Apply(new OrderPlaced(cmd.OrderId, cmd.Items));
    [EventHandler]   void On(OrderPlaced e) { Id = e.OrderId; _items = e.Items; }
}
```

### 253. Event Store на PostgreSQL (Marten-подход)
Таблица `events(stream_id, version, type, data jsonb, meta jsonb, global_seq bigserial)`; уникальный индекс `(stream_id, version)` = optimistic concurrency.

### 254. `global_seq` — единый порядок для проекций
BIGSERIAL + трюк с transaction-gap handling (как Marten) — проекции читают строго монотонно, без пропусков.

### 255. Многодвижковость стора
`IEventStore` реализации: PostgreSQL / SQL Server / EventStoreDB(Kurrent) / SQLite (edge) / InMemory — единые тесты соответствия.

### 256. Снапшоты по политикам
```csharp
es.Snapshots<OrderAggregate>(every: 100 events, or: TimeSpan.FromDays(7), compress: true);
```
Снапшот + хвост событий; инвалидация снапшотов при смене версии Evolve.

### 257. Inline / Async / Live проекции (Marten)
- **Inline** — в транзакции записи (строгая согласованность);
- **Async** — фоновый daemon с чекпоинтами;
- **Live** — fold на лету при чтении (для редких запросов).

### 258. Проекция как класс с мульти-стрим агрегацией
```csharp
public sealed class CustomerLtvProjection : Projection<CustomerLtv>
{
    public void On(OrderPaid e, CustomerLtv view) => view.Total += e.Amount;
    public void On(OrderRefunded e, CustomerLtv view) => view.Total -= e.Amount;
    public override string Key(object e) => ((dynamic)e).CustomerId; // генератор заменит на typed-map
}
```

### 259. Реплей проекций одной командой
`avtobus projections rebuild CustomerLtv --parallel 8` — teardown, реплей с global_seq=0, catch-up, атомарная подмена таблицы (blue/green таблиц).

### 260. Чекпоинты проекций с прогресс-метрикой
`avtobus.projection.lag` (событий позади головы) — алерты на отставание; дашборд с прогресс-барами реплея.

### 261. Версионированные проекции side-by-side
`CustomerLtv_v2` строится параллельно с рабочей v1; переключение readers — feature-флагом; v1 сносится после.

### 262. Upcasting событий при чтении (Axon)
Цепочки upcaster-ов применяются лениво при загрузке стрима — старые события в сторе не переписываются никогда.

### 263. Copy-transform миграции стора
Когда upcasting-а мало: `avtobus es migrate --transform RenameField` — копирование стора с трансформацией и dual-write окном.

### 264. Шифрование payload персональных данных (crypto-shredding)
Ключ per-subject (пользователь); «право на забвение» = удаление ключа, события остаются, PII нечитаемо:
```csharp
es.Encryption(e => e.PerSubject<UserRegistered>(x => x.UserId, fields: u => new { u.Email, u.Phone }));
```

### 265. Мета-данные событий стандартизированы
`meta: { causationId, correlationId, userId, tenantId, source, schemaV }` — автозаполнение из ConsumeContext.

### 266. Мультитенантный стор: conjoined vs separate
Режимы: колонка tenant_id (+RLS) / схема на тенанта / БД на тенанта — конфиг одной строкой (Marten-идея).

### 267. Архивация стримов (stream archiving)
Закрытые стримы (заказ доставлен год назад) → перенос в cold storage (S3 parquet) с заглушкой-указателем; горячая таблица компактна.

### 268. `$by_category` и системные проекции (EventStoreDB)
Автостримы `$ce-orders` (все заказы), `$et-OrderPlaced` (по типу) — для подписок и отладки.

### 269. Подписки на живые события стора (subscriptions)
`es.SubscribeFromNow<OrderPlaced>(...)` / `SubscribeFromBeginning` — интеграция с шиной: событие стора автоматически публикуется в брокер через outbox.

### 270. Событие-носитель состояния vs указатель — выбор политики
Publish полного события (ECST) или тонкого уведомления с self-link — конфиг per-event; гайд в доках, анализатор подсказывает.

### 271. Команда → стрим: строгая последовательность через партиции
Все команды одного агрегата — в одну партицию (идея 25): optimistic concurrency почти не срабатывает, ретраи редки.

### 272. `IRevisionedCommand` — ожидаемая версия в команде
Клиент передаёт `ExpectedVersion` (из прочитанной модели) — конфликт бизнес-уровня ловится до применения (real CQRS-consistency UX).

### 273. Wait-for-projection хелпер для UI
```csharp
await bus.SendAndWaitForProjection(cmd, projection: "OrderList", timeout: 3.Seconds());
```
API возвращает управление, когда read-модель догнала запись (читаем свой global_seq).

### 274. Идемпотентный append по MessageId
`AppendIfNotExists(streamId, messageId)` — повторная команда не задваивает события (дедуп на сторе).

### 275. Быстрый fold: генерируемые Evolve-диспетчеры
Source Generator создаёт switch по типам без виртуальных вызовов и боксинга — реплей миллиона событий агрегата за секунды.

### 276. Кэш агрегатов с инвалидацией по версии
`IAggregateCache`: горячие агрегаты в памяти; ключ = (id, version); append инвалидирует; L2 — Redis c версией.

### 277. Batch-append для импорта
`es.AppendBatch(streams)` — импорт истории (миграция с legacy) миллионами событий с отключением inline-проекций и последующим catch-up.

### 278. Темпоральные запросы «на момент времени»
```csharp
var order = await es.Load<OrderAggregate>(id, asOf: new DateTime(2025, 1, 1));
```
Состояние на дату — из событий до неё; для аудита и разбора инцидентов.

### 279. Time-travel отладчик
Дашборд: слайдер по версиям агрегата, дифф состояния между версиями, событие-виновник каждого изменения.

### 280. What-if реплей
Прогнать альтернативный Evolve/Decide на исторических событиях: «что если бы комиссия была 2%» — песочница для продуктовых гипотез.

### 281. Проекции в разные хранилища
Postgres (SQL-view), Elasticsearch (поиск), Redis (горячий кэш), ClickHouse (аналитика) — один класс проекции, несколько `IProjectionTarget`.

### 282. CDC-мост: outbox → Debezium-совместимый формат
Для команд, где уже есть Kafka Connect инфраструктура — outbox-таблица в формате Debezium Outbox Event Router.

### 283. Weak reference между агрегатами — только через события
Анализатор `AVB050`: прямая загрузка чужого агрегата в Decide — ошибка; читай проекцию или слушай событие.

### 284. Правило маленьких стримов
Метрика длины стрима; > 10k событий → рекомендация: закрывающее событие + новый стрим (паттерн «closing the books»), хелпер `es.CloseAndContinue(streamId)`.

### 285. Валидация «полноты» Evolve
Генератор проверяет: каждый тип события стрима обработан в Evolve или явно помечен `[IgnoredInFold]` — забытая ветка = ошибка компиляции.

### 286. Каталог событий с примерами из стора
Дашборд показывает по каждому типу события: частота, последний экземпляр (redacted), подписчики, проекции-потребители.

### 287. GDPR-отчёт по subject
`avtobus es subject-report user-42` — все события/проекции, содержащие данные субъекта (по crypto-shredding реестру ключей).

### 288. Компактные события: колоночное хранение архива
Экспорт в Parquet с типизированными колонками → аналитики читают историю Spark/DuckDB без нагрузки на прод.

### 289. Потоковая аналитика поверх стора: мини-DSL
```csharp
es.Stream<OrderPaid>()
  .Window(Tumbling(1.Hours()))
  .GroupBy(e => e.Region)
  .Aggregate(Sum(e => e.Amount))
  .Into(new HourlyRevenueProjection());
```
Kafka Streams-подобные окна поверх global_seq.

### 290. Экспорт «семплов» для тестов
`avtobus es sample orders --count 100 --anonymize` — реалистичные анонимизированные фикстуры для юнит-тестов.

### 291. Correlation-граф в дашборде
Визуализация цепочки: команда → события → команды → события (по Causation/Correlation) — дерево причинности инцидента за секунды.

### 292. Register-based агрегат для экстремальной нагрузки
Паттерн memory-image: агрегаты-акторы держат состояние в памяти, события — WAL (LMAX-идея); recovery — реплей; для бирж/игр.

### 293. Дет. симуляция кластера проекций
Тест-режим с виртуальным временем и сериализованной случайностью (FoundationDB-стиль) — гонки daemon-ов ловятся в CI.

### 294. Blue/green деплой async daemon
Два поколения демона проекций работают одновременно с разными чекпоинтами; переключение атомарно; rollback мгновенный.

### 295. Проекция «как таблица EF Core»
Read-модели — обычные EF-сущности; миграции EF управляют схемой; `DbContext` проекций отделён от доменного.

### 296. Наблюдаемость fold-а: медленные Evolve
Метрика времени Evolve per event type; аллокации; топ тяжёлых событий — оптимизация реплея данными.

### 297. Событийные вебхуки для партнёров
Подписка партнёра на подмножество событий с фильтром и трансформацией (убрать внутренние поля) + подпись HMAC + машина ретраев (идея 189).

### 298. Right-to-audit: неизменяемость доказуемо
Хэш-цепочка событий (`prev_hash` в metadata) + периодический анкоринг корневого хэша во внешний неизменяемый журнал — доказательство отсутствия незаметных правок истории.

### 299. Автогенерация read-API из проекций
`[ExposeAsApi("/api/orders")]` на проекции → минимальный REST (фильтры, пагинация, ETag по global_seq) через Source Generator.

### 300. Учебный режим: `avtobus es explain`
Интерактивное объяснение: «какие события создали это состояние», «почему команда отклонена (какой инвариант)» — onboarding новичков в ES за день.
