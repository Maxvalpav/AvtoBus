using AvtoBus;
using AvtoBus.Configuration;
using AvtoBus.InMemory;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddAvtoBus(bus =>
{
    bus.UseInMemory();
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
    bus.ServiceName("delivery");
    bus.Recoverability(r => r
        .ImmediateRetries(1)
        .DelayedRetries(3, Backoff.Exponential(TimeSpan.FromSeconds(1))));
});

await builder.Build().RunAsync();