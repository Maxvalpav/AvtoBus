# 💡 Идеи 201–250: Саги, workflow, планировщик

### 201. Сага как класс с состоянием (NServiceBus-style)
```csharp
public sealed class OrderSaga : Saga<OrderSagaState>,
    IStartedBy<OrderPlaced>, IHandle<PaymentCompleted>, IHandle<ShipmentDispatched>
{
    public Task Handle(OrderPlaced m)   { State.OrderId = m.OrderId; return Send(new RequestPayment(m.OrderId)); }
    public Task Handle(PaymentCompleted m) => Send(new CreateShipment(State.OrderId));
    public Task Handle(ShipmentDispatched m) { MarkComplete(); return Publish(new OrderFulfilled(State.OrderId)); }
}
```

### 202. Корреляция саги декларативно
```csharp
protected override void Correlate(SagaMap<OrderSagaState> map)
{
    map.On<OrderPlaced>(m => m.OrderId).StartsNew();
    map.On<PaymentCompleted>(m => m.OrderId);
}
```
Генератор строит индексы хранилища по correlation-полям.

### 203. Стейт-машина DSL (MassTransit Automatonymous, но проще)
```csharp
public sealed class OrderMachine : StateMachine<OrderState>
{
    public State AwaitingPayment { get; } = default!;
    public State Shipping { get; } = default!;

    public OrderMachine()
    {
        Initially(When<OrderPlaced>().Then(x => x.Send(new RequestPayment(x.Message.OrderId))).GoTo(AwaitingPayment));
        During(AwaitingPayment,
            When<PaymentCompleted>().GoTo(Shipping),
            When<PaymentFailed>().Then(x => x.Publish(new OrderCancelled(x.State.OrderId))).Finalize());
    }
}
```

### 204. Визуализация стейт-машины
Генератор экспортирует Mermaid/DOT диаграмму из кода машины; в дашборде — живая диаграмма с подсветкой текущего состояния каждого инстанса.

### 205. Durable Execution саги (Temporal-style)
Сага — обычный async-код; каждый await шины — checkpoint; после краша реплей продолжает с места остановки:
```csharp
public static async Task Run(OrderPlaced trigger, ISagaContext ctx)
{
    await ctx.Send(new RequestPayment(trigger.OrderId));
    var pay = await ctx.WaitFor<PaymentCompleted>(timeout: 30.Minutes());
    if (pay is null) { await ctx.Publish(new OrderCancelled(trigger.OrderId)); return; }
    await ctx.Send(new CreateShipment(trigger.OrderId));
}
```

### 206. Таймауты саги как first-class (NServiceBus timeouts)
`await ctx.RequestTimeout(new PaymentOverdue(), 24.Hours());` — durable, переживает рестарты, отменяется при завершении саги.

### 207. Компенсации в durable-сагах
```csharp
await ctx.Step(() => api.BookHotel(cmd), compensate: r => api.Cancel(r.BookingId));
await ctx.Step(() => api.BookFlight(cmd), compensate: r => api.Refund(r.TicketId));
// исключение → компенсации выполняются в обратном порядке
```

### 208. Хранилища саг: EF Core / Marten / Mongo / Redis / InMemory
Единый `ISagaStore` c optimistic concurrency (версия строки) — конкурентные сообщения одной саги не затирают друг друга.

### 209. Pessimistic-режим саги по ключу
Опция: сообщения одного инстанса саги обрабатываются строго последовательно через partition-actor (идея 25) — конкуренции нет вовсе.

### 210. Автоархив завершённых саг
`MarkComplete()` → строка переносится в архивную таблицу (или ES-стрим) для аудита, «горячая» таблица остаётся маленькой.

### 211. Сага-таймлайн в дашборде
Полная история инстанса: сообщения, переходы, таймауты, ретраи — на одной временной шкале (вдохновение: Temporal UI).

### 212. Заморозка/разморозка инстанса саги
`avtobus saga pause <id>` — инцидент-менеджмент: остановить проблемный процесс, починить данные, продолжить.

### 213. Массовые операции над сагами
`avtobus saga query 'State=AwaitingPayment AND Age>7d' --send PaymentOverdue` — bulk-вмешательство.

### 214. Версионирование саг
`SagaVersion` в состоянии; при загрузке старой версии — миграция состояния (upcaster для саг); параллельная работа V1/V2 для in-flight процессов.

### 215. Сага-«молния»: короткие саги в одном сообщении
Если все шаги локальны и быстры, оркестрация схлопывается в один хендлер (оптимизация codegen) — без промежуточных сообщений.

### 216. Роутинг-слипы (routing slip, MassTransit Courier)
Итинерарий шагов едет в самом сообщении; каждый узел выполняет свой шаг и передаёт дальше; компенсации — в обратном порядке по слипу:
```csharp
var slip = RoutingSlip.Build(b => {
    b.AddActivity("reserve", "inventory-service");
    b.AddActivity("charge", "payment-service");
    b.AddActivity("ship", "shipping-service");
});
```

### 217. Выбор: оркестрация vs хореография — гайд и шаблоны
`avtobus new saga --style orchestration|choreography` — генерит скелет с правильными паттернами и тестами.

### 218. Процесс-менеджер с бизнес-календарём
`ctx.RequestTimeout(evt, BusinessTime.Days(3, calendar: "ru-RU"))` — «3 рабочих дня» с учётом праздников.

### 219. Человеческий шаг в саге (human-in-the-loop)
```csharp
var approval = await ctx.WaitForHuman<ManagerApproval>(assignee: order.ManagerId, timeout: 2.Days());
```
Генерирует task в интеграции (email/Slack/UI), продолжает по ответу.

### 220. Дочерние саги
`await ctx.StartChildSaga<RefundSaga>(new StartRefund(orderId));` — родитель ждёт результата или подписывается на завершение.

### 221. Сага-мониторы (health процессов)
Декларативный SLA: `Saga<OrderSagaState>.Sla(from: OrderPlaced, to: OrderFulfilled, max: 2.Hours())` → метрика + алерт по «застрявшим» бизнес-процессам. Killer-фича для бизнеса.

### 222. Идемпотентный старт саги
Повторный `IStartedBy`-message для существующей корреляции — конфигурируемо: игнор / ошибка / рестарт (по умолчанию игнор + лог).

### 223. Cron-сообщения (Oban/Quartz)
```csharp
bus.Schedule.Cron<GenerateDailyReport>("0 6 * * *", tz: "Europe/Moscow");
```
Durable, single-fire в кластере (advisory lock), misfire-политики.

### 224. Distributed single-instance джобы
Лидер-элекшн для cron через PostgreSQL advisory locks / Redis RedLock / K8s Lease — без ZooKeeper.

### 225. Календарные расписания сложнее cron
`Schedule.Every(1.Months()).OnLastBusinessDay().At("18:00")` — fluent-расписания с бизнес-календарями.

### 226. Отложенные сообщения с точностью и переносом
Хранение в durable-таблице с индексом по времени; при рестарте догоняем просроченные с политикой: fire-all / fire-latest / skip.

### 227. Rate-limited шедулинг рассылок
`bus.ScheduleSpread(messages, over: 2.Hours())` — 100k пушей размазываются равномерно, не убивая downstream.

### 228. Временные (temporal) правила подписки
`bus.Consumer<T>().ActiveWindow("09:00-21:00", tz)` — ночной трафик копится в очереди и обрабатывается утром (для интеграций с «спящими» партнёрами).

### 229. Сага-тестирование: given/when/then харнесс
```csharp
await SagaScenario.For<OrderSaga>()
    .Given(new OrderPlaced(id))
    .When(new PaymentCompleted(id))
    .ThenSent<CreateShipment>(m => m.OrderId == id)
    .ThenState(s => s.Status == "Shipping")
    .Run();
```

### 230. Симуляция времени в тестах саг
`scenario.AdvanceTime(25.Hours())` — таймауты срабатывают мгновенно в виртуальных часах (`TimeProvider` .NET 8+ повсюду).

### 231. Свод правил анти-паттернов саг (анализатор)
`AVB040`: запрос данных из чужого сервиса внутри саги (вместо данных в событии); `AVB041`: сага без таймаутов на внешние шаги.

### 232. Outbox-интеграция саги
Изменение состояния саги + исходящие команды — одна транзакция (сага-store и outbox в одной БД по умолчанию).

### 233. Приоритезация инстансов саг
VIP-клиент → сообщения его саг через high-priority маршрут (идея 188), политика на уровне корреляции.

### 234. Сага с «мягким» завершением
`MarkCompleteAfter(7.Days())` — сага завершена, но ещё принимает поздние дубликаты/хвосты без реанимации процесса.

### 235. Экспорт процесса в BPMN
Генератор строит BPMN 2.0 XML из стейт-машины — аналитики видят процесс в Camunda Modeler без чтения кода.

### 236. Watchdog осиротевших саг
Инстансы без активности N дней и не complete → отчёт + опциональная политика (напомнить/компенсировать/архив).

### 237. Компактное состояние: MemoryPack для saga-state
Бинарная сериализация состояния с версионированием — 5–10x меньше JSON в горячих саговых таблицах.

### 238. Изоляция данных саги (row-level security)
Saga-state таблицы с RLS по `TenantId` — сага одного тенанта физически не прочитает чужое состояние.

### 239. Реактивные джойны событий
```csharp
bus.When<InvoiceCreated>().And<PaymentReceived>(join: (a, b) => a.InvoiceId == b.InvoiceId, within: 1.Hours())
   .Then((a, b) => new InvoicePaid(a.InvoiceId));
```
Лёгкий коррелятор без полноценной саги (Kafka Streams join, но проще).

### 240. Escalation-цепочки таймаутов
`Timeout(30m → NotifyManager) → Timeout(2h → NotifyDirector) → Timeout(1d → AutoCancel)` — декларативная лестница эскалаций.

### 241. Сохранение прогресса больших batch-джобов
Джоб «обработай 1M строк» → чанки-сообщения с курсором в состоянии саги; краш → продолжение с последнего чанка (idea: AWS Step Functions Map).

### 242. Сага-шаблоны индустрии
Готовые NuGet-шаблоны: `OrderFulfillmentSaga`, `PaymentRetrySaga`, `DocumentApprovalSaga`, `OnboardingSaga` — параметризуемые, с тестами.

### 243. Отчёт «воронка процесса»
Автоагрегация: сколько инстансов на каждом состоянии, конверсия переходов, p95 длительности шага — бизнес-аналитика из коробки.

### 244. Персональный «продолжатель» после deploy
После выката новой версии стейт-машины in-flight инстансы валидируются: недостижимые состояния → отчёт мигратора.

### 245. Ограничение параллельных инстансов
`MaxConcurrentInstances(1000)` на тип саги — новые старты выше лимита ждут в очереди (bulkhead для тяжёлых процессов).

### 246. Ретроспективный старт саги из истории
Реплей исторических событий в новую сагу (backfill процессов после внедрения нового флоу) с виртуальными часами.

### 247. Инварианты состояния саги
```csharp
protected override void Invariants(SagaInvariants<OrderSagaState> inv)
    => inv.Assert(s => s.PaidAmount <= s.TotalAmount, "overpayment");
```
Нарушение — стоп саги + алерт (никогда не тихая порча данных).

### 248. Сага как источник событий (event-sourced saga)
Состояние = fold событий саги; полный аудит решений процесса бесплатно; интеграция с AvtoBus.EventSourcing.

### 249. Оффлайн-симулятор процессов
`avtobus saga simulate OrderSaga --events sample.jsonl` — прогон исторических данных через новую версию машины: сколько бы пошло по каким веткам.

### 250. Слияние дублей саг
Инструмент merge инстансов при позднем обнаружении дублированной корреляции (двойной OrderId из-за бага) — с журналом решений.
