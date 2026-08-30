using AvtoBus;
using AvtoBus.InMemory;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddAvtoBus(bus =>
{
    bus.UseInMemory();
    bus.AddConsumersFromAssembly(typeof(Program).Assembly);
    bus.ServiceName("addresses");
});

await builder.Build().RunAsync();