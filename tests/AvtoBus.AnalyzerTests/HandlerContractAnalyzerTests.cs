using AvtoBus.Analyzers;

namespace AvtoBus.AnalyzerTests;

public class HandlerContractAnalyzerTests
{
    [Fact]
    public void String_first_parameter_reports_AVB008()
    {
        var source = """
            using System.Threading.Tasks;

            public static class Handlers
            {
                public static Task Handle(string text) => Task.CompletedTask;
            }
            """;

        var result = AnalyzerDriver.Run(source, new HandlerContractAnalyzer());

        Assert.True(result.Has("AVB008"));
    }

    [Fact]
    public void Primitive_enum_and_guid_parameters_report_AVB008()
    {
        var source = """
            using System;
            using System.Threading.Tasks;

            public static class Handlers
            {
                public static Task Handle(int value) => Task.CompletedTask;
                public static Task Consume(DayOfWeek day) => Task.CompletedTask;
                public static Task HandleAsync(Guid id) => Task.CompletedTask;
            }
            """;

        var result = AnalyzerDriver.Run(source, new HandlerContractAnalyzer());

        Assert.Equal(3, result.OfId("AVB008").Count());
    }

    [Fact]
    public void Contract_first_parameter_is_clean()
    {
        var source = """
            using System.Threading.Tasks;
            using AvtoBus;

            public sealed record PlaceOrder(string OrderId) : ICommand;

            public static class Handlers
            {
                public static Task Handle(PlaceOrder command) => Task.CompletedTask;
            }
            """;

        var result = AnalyzerDriver.Run(source, new HandlerContractAnalyzer());

        Assert.DoesNotContain(result.Diagnostics, d => d.Id == "AVB008");
    }

    [Fact]
    public void Interface_first_parameter_is_not_reported()
    {
        // Полиморфный параметр — намеренный отказ генератора, не ошибка контракта.
        var source = """
            using System.Threading.Tasks;

            public interface IOrderEvent { }

            public static class Handlers
            {
                public static Task Handle(IOrderEvent evt) => Task.CompletedTask;
            }
            """;

        var result = AnalyzerDriver.Run(source, new HandlerContractAnalyzer());

        Assert.DoesNotContain(result.Diagnostics, d => d.Id == "AVB008");
    }

    [Fact]
    public void Non_handler_methods_are_not_reported()
    {
        var source = """
            using System.Threading.Tasks;

            public static class NotHandler
            {
                public static Task Calculate(int value) => Task.CompletedTask;
                public static Task Process(string text) => Task.CompletedTask;
            }
            """;

        var result = AnalyzerDriver.Run(source, new HandlerContractAnalyzer());

        Assert.DoesNotContain(result.Diagnostics, d => d.Id == "AVB008");
    }
}
