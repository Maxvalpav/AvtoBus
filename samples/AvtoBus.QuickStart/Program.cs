using System.ComponentModel.DataAnnotations;
using AvtoBus;
using AvtoBus.Configuration;
using AvtoBus.Dashboard;
using AvtoBus.Outbox.EfCore;
using AvtoBus.QuickStart.Data;
using AvtoBus.RabbitMq;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Outbox-интерсептор подключается к DbContext: SaveChanges пишет order + outbox-запись атомарно.
builder.Services.AddDbContext<OrderDbContext>((sp, opt) => opt
    .UseNpgsql(builder.Configuration.GetConnectionString("Db")!)
    .AddInterceptors(sp.GetRequiredService<OutboxSaveChangesInterceptor>()));

builder.Services.AddAvtoBus(bus =>
{
    // 1) Транспорт
    bus.UseRabbitMq(opt => opt.ConnectionString = builder.Configuration.GetConnectionString("Rabbit")!);

    // 2) Надёжность: transactional outbox + inbox-дедупликация
    bus.UseOutbox<OrderDbContext>(o => o.BatchSize = 200);
    bus.UseInboxDeduplication(TimeSpan.FromHours(24));

    // 3) Recoverability: мгновенные + отложенные ретраи с backoff
    bus.Recoverability(r => r
        .ImmediateRetries(3)
        .DelayedRetries(5, Backoff.Exponential(TimeSpan.FromSeconds(5)))
        .MapException<ValidationException>(FailureAction.Discard));

    // 4) Хендлеры из этой сборки (Source Generator найдёт их автоматически)
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
    bus.ServiceName("order-service");
});

// Дашборд шины
builder.Services.AddAvtoBusDashboard();

var app = builder.Build();

app.MapGet("/", () => "OrderService running");
app.MapAvtoBusDashboard(); // RoutePrefix по умолчанию = "/bus"

app.Run();
