# Миграция MassTransit -> AvtoBus Power

> Фича 5 по порядку — доки migration (fable 13).

## Замена регистрации

MassTransit:
  
services.AddMassTransit(x=>{ x.AddConsumers(typeof(Program).Assembly); x.UsingRabbitMq((ctx,cfg)=>{ cfg.Host("localhost",h=>{h.Username("guest");}); cfg.ConfigureEndpoints(ctx); });});
  
AvtoBus:
  
services.AddAvtoBus(bus=>{ bus.UseRabbitMq(o=>o.ConnectionString="amqp://guest:guest@localhost"); bus.AddConsumersFromAssembly(typeof(Program).Assembly); });
  

## Замена вызова

MT: IPublishEndpoint.Publish(event) / ISendEndpoint.Send(cmd) / IRequestClient<T>.GetResponse<TReply>
AvtoBus: IBus.PublishAsync(event) / IBus.SendAsync(cmd) / IBus.RequestAsync<TReq,TRep>(req) — 1 интерфейс IBus.cs:9

## Consumer

MT: class X : IConsumer<T>{ Task Consume(ConsumeContext<T> ctx)}
AvtoBus: class X : IConsumer<T>{ Task ConsumeAsync(ConsumeContext<T> ctx)} или static Task Handle(T msg, ConsumeContext ctx) — без интерфейса, AOT

## Outbox

MT: x.AddEntityFrameworkOutbox<AppDb>(o=>o.UseSqlServer()) + modelBuilder.AddOutboxMessageEntity()
AvtoBus: us.UseOutbox<AppDb>() + services.AddDbContext<AppDb>(o=>o.UseNpgsql(...).AddInterceptors(sp.GetRequiredService<OutboxSaveChangesInterceptor>()))

## Retry/DLQ

MT: cfg.UseMessageRetry(r=>r.Interval(3, TimeSpan.FromSeconds(1)))
AvtoBus: us.Recoverability(r=>r.ImmediateRetries(3).DelayedRetries(5, Backoff.Exponential(TimeSpan.FromSeconds(5)))) + us.UseInboxDeduplication()

## См также

- fable-ref/13-migration-cookbook.md — полный side-by-side 4 фазы
- POWER_VS_ALTERNATIVES.md — таблица MT vs AvtoBus
