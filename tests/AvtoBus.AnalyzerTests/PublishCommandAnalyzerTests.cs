using AvtoBus.Analyzers;

namespace AvtoBus.AnalyzerTests;

public class PublishCommandAnalyzerTests
{
    private const string ContractBoilerplate =
        """
        using AvtoBus;

        public class PlaceOrder : ICommand { }
        public class OrderPlaced : IEvent { }
        """;

    [Fact]
    public void Publish_command_reports_AVB004()
    {
        var source = ContractBoilerplate +
            """
            public static class Producer
            {
                public static void Go(IBus bus) => bus.PublishAsync(new PlaceOrder());
            }
            """;

        var result = AnalyzerDriver.Run(source, new PublishCommandAnalyzer());

        Assert.True(result.Has("AVB004"));
    }

    [Fact]
    public void Send_event_reports_AVB005()
    {
        var source = ContractBoilerplate +
            """
            public static class Producer
            {
                public static void Go(IBus bus) => bus.SendAsync(new OrderPlaced());
            }
            """;

        var result = AnalyzerDriver.Run(source, new PublishCommandAnalyzer());

        Assert.True(result.Has("AVB005"));
    }

    [Fact]
    public void Send_command_and_publish_event_are_clean()
    {
        var source = ContractBoilerplate +
            """
            public static class Producer
            {
                public static void Go(IBus bus)
                {
                    bus.SendAsync(new PlaceOrder());
                    bus.PublishAsync(new OrderPlaced());
                }
            }
            """;

        var result = AnalyzerDriver.Run(source, new PublishCommandAnalyzer());

        Assert.DoesNotContain(result.Diagnostics, d => d.Id is "AVB004" or "AVB005");
    }

    [Fact]
    public void Null_contract_is_ignored()
    {
        var source = """
            using AvtoBus;
            public static class Producer
            {
                public static void Go(IBus bus) => bus.PublishAsync(new object());
            }
            """;

        var result = AnalyzerDriver.Run(source, new PublishCommandAnalyzer());

        Assert.DoesNotContain(result.Diagnostics, d => d.Id is "AVB004" or "AVB005");
    }
}
