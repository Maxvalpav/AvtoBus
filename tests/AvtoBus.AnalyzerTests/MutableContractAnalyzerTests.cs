using AvtoBus.Analyzers;

namespace AvtoBus.AnalyzerTests;

public class MutableContractAnalyzerTests
{
    [Fact]
    public void Settable_property_reports_AVB010()
    {
        var source = """
            using AvtoBus;
            public class OrderPlaced : IEvent
            {
                public string OrderId { get; set; } = "";
            }
            """;

        var result = AnalyzerDriver.Run(source, new MutableContractAnalyzer());

        Assert.True(result.Has("AVB010"));
    }

    [Fact]
    public void Init_only_property_is_clean()
    {
        var source = """
            using AvtoBus;
            public class OrderPlaced : IEvent
            {
                public required string OrderId { get; init; }
            }
            """;

        var result = AnalyzerDriver.Run(source, new MutableContractAnalyzer());

        Assert.DoesNotContain(result.Diagnostics, d => d.Id == "AVB010");
    }

    [Fact]
    public void TenantId_in_body_reports_AVB017()
    {
        var source = """
            using AvtoBus;
            public class OrderPlaced : IEvent
            {
                public required string TenantId { get; init; }
            }
            """;

        var result = AnalyzerDriver.Run(source, new MutableContractAnalyzer());

        Assert.True(result.Has("AVB017"));
    }

    [Fact]
    public void God_event_reports_AVB022()
    {
        var props = string.Join("\n    ", Enumerable.Range(1, 25).Select(i => $"public int P{i} {{ get; init; }}"));
        var source = string.Concat(
            "using AvtoBus;\n",
            "public class OrderUpdated : IEvent\n",
            "{\n",
            props,
            "\n}\n");

        var result = AnalyzerDriver.Run(source, new MutableContractAnalyzer());

        Assert.True(result.Has("AVB022"));
    }

    [Fact]
    public void Plain_class_is_ignored()
    {
        var source = """
            public class Order
            {
                public string Name { get; set; } = "";
            }
            """;

        var result = AnalyzerDriver.Run(source, new MutableContractAnalyzer());

        Assert.DoesNotContain(result.Diagnostics, d => d.Id is "AVB010" or "AVB017" or "AVB022");
    }
}
