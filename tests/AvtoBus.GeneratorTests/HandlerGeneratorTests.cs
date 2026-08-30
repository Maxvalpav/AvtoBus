namespace AvtoBus.GeneratorTests;

public class HandlerGeneratorTests
{
    private const string Contracts = """
        namespace TestContracts;

        public sealed record PlaceOrder(string OrderId, decimal Total) : AvtoBus.ICommand;
        public sealed record OrderPlaced(string OrderId) : AvtoBus.IEvent;
        public sealed record PaymentCompleted(string OrderId) : AvtoBus.IEvent;
        """;

    private static string Handlers(string body) => """
        #nullable enable
        using AvtoBus;
        using System.Threading;
        using TestContracts;

        namespace Handlers;

        """ + body;

    private GeneratorResult Run(params string[] handlerFiles)
        => GeneratorDriver.Run([Contracts, .. handlerFiles]);

    [Fact]
    public void EmitsDispatcher_ForStaticHandleMethod()
    {
        var result = Run(Handlers("""
            public static class OrderHandlers
            {
                public static Task Handle(PlaceOrder command) => Task.CompletedTask;
            }
            """));

        Assert.True(result.HasSource("OrderHandlers_PlaceOrder_Dispatcher.g.cs"), "Диспетчер не сгенерирован");
        Assert.Contains("IMessageDispatcher", result.Source("OrderHandlers_PlaceOrder_Dispatcher.g.cs"));
        Assert.Contains("typeof(global::TestContracts.PlaceOrder)", result.Source("OrderHandlers_PlaceOrder_Dispatcher.g.cs"));
    }

    [Fact]
    public void EmitsModuleInitializer_WithRegistration()
    {
        var result = Run(Handlers("""
            public static class OrderHandlers
            {
                public static Task Handle(PlaceOrder command) => Task.CompletedTask;
            }
            """));

        Assert.True(result.HasSource("AvtoBusModuleInit.g.cs"));
        var registration = result.Source("AvtoBusModuleInit.g.cs");
        Assert.Contains("AvtoBusRegistry.Register", registration);
        Assert.Contains("typeof(global::Handlers.OrderHandlers)", registration);
    }

    [Fact]
    public void EmitsDispatcher_ForIConsumerInterface()
    {
        var result = Run(Handlers("""
            public sealed class OrderPlacedConsumer : IConsumer<OrderPlaced>
            {
                public Task ConsumeAsync(ConsumeContext<OrderPlaced> context) => Task.CompletedTask;
            }
            """));

        Assert.True(result.HasSource("OrderPlacedConsumer_OrderPlaced_Dispatcher.g.cs"));
        Assert.Contains("ConsumeAsync", result.Source("OrderPlacedConsumer_OrderPlaced_Dispatcher.g.cs"));
    }

    [Fact]
    public void AsyncTask_InstanceHandler_GeneratesCompilingDispatcher()
    {
        // Регрессия: ReturnType с префиксом global:: не совпадал с голым
        // "System.Threading.Tasks.Task", и async-хендлер эмитил
        // "var __result = await Handler.Handle(...)" — CS0815 (assign void to var).
        var result = Run(Handlers("""
            public sealed class OrderHandler
            {
                public async Task Handle(PlaceOrder command, ConsumeContext ctx)
                {
                    await Task.CompletedTask;
                }
            }
            """));

        var source = result.Source("OrderHandler_PlaceOrder_Dispatcher.g.cs");
        Assert.True(result.HasSource("OrderHandler_PlaceOrder_Dispatcher.g.cs"), "Диспетчер не сгенерирован");
        Assert.DoesNotContain("var __result = await", source);
        Assert.Contains("await __handler.Handle", source);
    }

    [Fact]
    public void DoesNotEmitJsonSerializerContext_UserProvidesOwn()
    {
        // STJ-генератор не обрабатывает типы, созданные другими генераторами (CS0534),
        // поэтому контекст [JsonSerializable] должен создаваться в исходниках пользователя.
        var result = Run(Handlers("""
            public static class OrderHandlers
            {
                public static Task Handle(PlaceOrder command) => Task.CompletedTask;
                public static Task Consume(OrderPlaced @event) => Task.CompletedTask;
            }
            """));

        Assert.False(result.HasSource("AvtoBusJsonContext.g.cs"));
    }

    [Fact]
    public void ReportsAVB001_WhenCommandHasNoHandler()
    {
        var result = Run(Contracts, """
            using AvtoBus;
            using System.Threading.Tasks;
            using TestContracts;

            namespace Sender;

            public static class CommandSender
            {
                public static async Task RunAsync(IBus bus)
                    => await bus.SendAsync(new PlaceOrder("o-1", 10m));
            }
            """);

        Assert.Contains(result.Errors, d => d.Id == "AVB001");
    }

    [Fact]
    public void DoesNotReportAVB001_WhenCommandHasHandler()
    {
        var result = Run(
            Contracts,
            Handlers("""
                public static class OrderHandlers
                {
                    public static Task Handle(PlaceOrder command) => Task.CompletedTask;
                }
                """),
            """
            using AvtoBus;
            using System.Threading.Tasks;
            using TestContracts;

            namespace Sender;

            public static class CommandSender
            {
                public static async Task RunAsync(IBus bus)
                    => await bus.SendAsync(new PlaceOrder("o-1", 10m));
            }
            """);

        Assert.DoesNotContain(result.Errors, d => d.Id == "AVB001");
    }

    [Fact]
    public void ReportsAVB002_WhenCommandHasMultipleHandlers()
    {
        var result = Run(
            Handlers("""
                public static class FirstHandler
                {
                    public static Task Handle(PlaceOrder command) => Task.CompletedTask;
                }
                """),
            Handlers("""
                public static class SecondHandler
                {
                    public static Task Handle(PlaceOrder command) => Task.CompletedTask;
                }
                """));

        Assert.Contains(result.Errors, d => d.Id == "AVB002");
    }

    [Fact]
    public void ReportsAVB008_WhenFirstParameterIsNotAMessageContract()
    {
        // Handle(string)/Handle(int) и т.п. не должны стать хендлерами (расхождение с runtime
        // BusConfigurator.IsPlausibleMessageType) и обязаны давать AVB008 на этапе сборки.
        var result = Run(Handlers("""
            public static class BadHandler
            {
                public static Task Handle(string text) => Task.CompletedTask;
            }
            """));

        Assert.Contains(result.Errors, d => d.Id == "AVB008");
        Assert.DoesNotContain(result.Sources.Keys, k => k.EndsWith("_Dispatcher.g.cs"));
        Assert.DoesNotContain(result.Sources.Keys, k => k == "AvtoBusModuleInit.g.cs");
    }

    [Fact]
    public void ReportsAVB008_ForPrimitiveEnumGuidAndArrayParams()
    {
        var result = Run(
            Handlers("""
                public static class BadHandler
                {
                    public static Task Handle(int value) => Task.CompletedTask;
                }
                """),
            Handlers("""
                public static class BadEnumHandler
                {
                    public static Task Consume(System.DayOfWeek day) => Task.CompletedTask;
                }
                """),
            Handlers("""
                public static class BadGuidHandler
                {
                    public static Task HandleAsync(System.Guid id) => Task.CompletedTask;
                }
                """),
            Handlers("""
                public static class BadArrayHandler
                {
                    public static Task ConsumeAsync(byte[] payload) => Task.CompletedTask;
                }
                """));

        var avb008 = result.Errors.Where(d => d.Id == "AVB008").ToList();
        Assert.NotEmpty(avb008);
        Assert.Contains(avb008, d => d.GetMessage().Contains("int"));
        Assert.Contains(avb008, d => d.GetMessage().Contains("System.DayOfWeek"));
        Assert.Contains(avb008, d => d.GetMessage().Contains("System.Guid"));
        Assert.Contains(avb008, d => d.GetMessage().Contains("byte[]"));
    }

    [Fact]
    public void DoesNotReportAVB008_WhenFirstParameterIsContract()
    {
        var result = Run(Handlers("""
            public static class OrderHandlers
            {
                public static Task Handle(PlaceOrder command) => Task.CompletedTask;
            }
            """));

        Assert.DoesNotContain(result.Errors, d => d.Id == "AVB008");
        Assert.True(result.HasSource("OrderHandlers_PlaceOrder_Dispatcher.g.cs"));
    }

    [Fact]
    public void ReportsAVB010_WhenContractHasMutableProperties()
    {
        var result = Run(Handlers("""
            public sealed class MutableContract : IEvent
            {
                public string Name { get; set; } = "";
            }

            public static class Handler
            {
                public static Task Consume(MutableContract evt) => Task.CompletedTask;
            }
            """));

        Assert.Contains(result.Warnings, d => d.Id == "AVB010");
    }

    [Fact]
    public void DoesNotEmitAnything_ForNonHandlerMethods()
    {
        var result = Run(Handlers("""
            public static class NotHandler
            {
                public static Task Calculate(int value) => Task.CompletedTask;
                public static Task Process(string text) => Task.CompletedTask;
            }
            """));

        Assert.DoesNotContain(result.Sources.Keys, k => k.EndsWith("_Dispatcher.g.cs"));
        Assert.DoesNotContain(result.Sources.Keys, k => k == "AvtoBusModuleInit.g.cs");
    }

    [Fact]
    public void InjectsDependencies_FromConsumeContextServices()
    {
        var result = Run(Handlers("""
            public sealed class GreetingService
            {
                public string Greet() => "hello";
            }

            public static class OrderHandlers
            {
                public static Task Handle(PlaceOrder command, GreetingService greeting, CancellationToken ct)
                {
                    return Task.CompletedTask;
                }
            }
            """));

        var dispatcher = result.Source("OrderHandlers_PlaceOrder_Dispatcher.g.cs");
        Assert.Contains("ctx.Services.GetRequiredService<global::Handlers.GreetingService>()", dispatcher);
        Assert.Contains("ctx.CancellationToken", dispatcher);
    }
}
