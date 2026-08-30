using AvtoBus.Analyzers;

namespace AvtoBus.AnalyzerTests;

public class NamingAnalyzerTests
{
    [Fact]
    public void Past_tense_event_is_clean()
    {
        var source = """
            using AvtoBus;
            public class OrderPlaced : IEvent { }
            """;

        var result = AnalyzerDriver.Run(source, new NamingAnalyzer());

        Assert.DoesNotContain(result.Diagnostics, d => d.Id == "AVB060");
    }

    [Fact]
    public void Future_tense_event_reports_AVB060()
    {
        var source = """
            using AvtoBus;
            public class PlaceOrder : IEvent { }
            """;

        var result = AnalyzerDriver.Run(source, new NamingAnalyzer());

        Assert.True(result.Has("AVB060"));
    }

    [Fact]
    public void Event_suffix_reports_AVB060()
    {
        var source = """
            using AvtoBus;
            public class OrderPlacedEvent : IEvent { }
            """;

        var result = AnalyzerDriver.Run(source, new NamingAnalyzer());

        Assert.True(result.Has("AVB060"));
    }

    [Fact]
    public void Command_is_not_checked()
    {
        var source = """
            using AvtoBus;
            public class PlaceOrder : ICommand { }
            """;

        var result = AnalyzerDriver.Run(source, new NamingAnalyzer());

        Assert.DoesNotContain(result.Diagnostics, d => d.Id == "AVB060");
    }
}
