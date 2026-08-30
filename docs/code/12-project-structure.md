# Структура решения, .csproj и сборка

Полная структура NuGet-solution с файлами проектов.

---

## Структура репозитория

```
avtobus/
├── AvtoBus.slnx                          # Solution (новый XML-формат .NET 10)
├── Directory.Build.props                 # Общие настройки
├── Directory.Packages.props              # Central Package Management
├── global.json                           # Версия SDK
├── nuget.config
├── .editorconfig
├── LICENSE                               # MIT
├── README.md
├── SECURITY.md
│
├── src/
│   ├── AvtoBus.Core/
│   │   ├── AvtoBus.Core.csproj
│   │   ├── Envelope.cs
│   │   ├── IBus.cs
│   │   ├── DefaultBus.cs
│   │   ├── ConsumeContext.cs
│   │   ├── Markers.cs
│   │   ├── Attributes.cs
│   │   ├── BusHost.cs
│   │   ├── BusState.cs
│   │   ├── Metrics.cs
│   │   ├── Diagnostics.cs
│   │   ├── Router.cs
│   │   ├── TypeResolver.cs
│   │   ├── Result.cs
│   │   ├── OutgoingMessages.cs
│   │   ├── AvtoBusRegistry.cs
│   │   ├── Pipeline/
│   │   │   ├── IBusMiddleware.cs
│   │   │   ├── BusPipelineBuilder.cs
│   │   │   ├── TelemetryMiddleware.cs
│   │   │   ├── ScopeMiddleware.cs
│   │   │   ├── TenantMiddleware.cs
│   │   │   ├── InboxDedupMiddleware.cs
│   │   │   ├── RecoverabilityMiddleware.cs
│   │   │   └── HandlerInvokerMiddleware.cs
│   │   ├── Dispatching/
│   │   │   ├── IMessageDispatcher.cs
│   │   │   ├── DispatcherRegistry.cs
│   │   │   └── ReflectionDispatcherBuilder.cs
│   │   ├── Serialization/
│   │   │   ├── ISerializer.cs
│   │   │   └── DefaultJsonSerializer.cs
│   │   ├── Subscription/
│   │   │   ├── ISubscriptionCatalog.cs
│   │   │   └── ReflectionSubscriptionCatalog.cs
│   │   └── Transport/
│   │       ├── ITransport.cs
│   │       ├── TopologyPlan.cs
│   │       └── InMemory/
│   │           ├── InMemoryTransport.cs
│   │           ├── InMemoryQueue.cs
│   │           └── DelayScheduler.cs
│   │
│   ├── AvtoBus/                          # Метапакет
│   │   └── AvtoBus.csproj
│   ├── AvtoBus.RabbitMq/
│   ├── AvtoBus.Kafka/
│   ├── AvtoBus.Nats/
│   ├── AvtoBus.Redis/
│   ├── AvtoBus.Outbox.EfCore/
│   ├── AvtoBus.Sagas/
│   ├── AvtoBus.Scheduling/
│   ├── AvtoBus.EventSourcing/
│   ├── AvtoBus.Generators/              # Source Generator (netstandard2.0)
│   ├── AvtoBus.Analyzers/
│   ├── AvtoBus.Testing/
│   ├── AvtoBus.Dashboard/
│   └── AvtoBus.Cli/
│
├── tests/
│   ├── AvtoBus.Core.Tests/
│   ├── AvtoBus.Outbox.Tests/
│   ├── AvtoBus.Sagas.Tests/
│   ├── AvtoBus.Generators.Tests/
│   ├── AvtoBus.Integration.Tests/       # Testcontainers
│   └── AvtoBus.Conformance/             # Conformance-kit
│
├── benchmarks/
│   └── AvtoBus.Benchmarks/
│
├── samples/
│   ├── QuickStart/
│   └── ECommerce/
│
└── .github/
    └── workflows/
        ├── ci.yml
        ├── release.yml
        └── perf.yml
```

---

## global.json

```json
{
  "sdk": {
    "version": "10.0.100",
    "rollForward": "latestMinor"
  }
}
```

---

## Directory.Build.props

```xml
<Project>

  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <EnforceCodeStyleInBuild>true</EnforceCodeStyleInBuild>
    <AnalysisLevel>latest-recommended</AnalysisLevel>

    <!-- Deterministic builds -->
    <Deterministic>true</Deterministic>
    <ContinuousIntegrationBuild Condition="'$(GITHUB_ACTIONS)' == 'true'">true</ContinuousIntegrationBuild>

    <!-- Symbols -->
    <IncludeSymbols>true</IncludeSymbols>
    <SymbolPackageFormat>snupkg</SymbolPackageFormat>
    <EmbedUntrackedSources>true</EmbedUntrackedSources>
    <PublishRepositoryUrl>true</PublishRepositoryUrl>
  </PropertyGroup>

  <!-- NuGet metadata -->
  <PropertyGroup>
    <Authors>AvtoBus Contributors</Authors>
    <Company>AvtoBus</Company>
    <Product>AvtoBus</Product>
    <Copyright>Copyright © AvtoBus Contributors</Copyright>
    <PackageLicenseExpression>MIT</PackageLicenseExpression>
    <RepositoryType>git</RepositoryType>
    <PackageIcon>icon.png</PackageIcon>
    <PackageReadmeFile>README.md</PackageReadmeFile>
    <PackageTags>eda;event-driven;messaging;bus;cqrs;event-sourcing;saga;rabbitmq;kafka;outbox</PackageTags>
    <VersionPrefix>0.1.0</VersionPrefix>
  </PropertyGroup>

  <!-- Source Link -->
  <ItemGroup>
    <PackageReference Include="Microsoft.SourceLink.GitHub" PrivateAssets="All" />
  </ItemGroup>

</Project>
```

---

## Directory.Packages.props (Central Package Management)

```xml
<Project>

  <PropertyGroup>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
    <CentralPackageTransitivePinningEnabled>true</CentralPackageTransitivePinningEnabled>
  </PropertyGroup>

  <ItemGroup>
    <!-- Microsoft.Extensions -->
    <PackageVersion Include="Microsoft.Extensions.DependencyInjection" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Hosting.Abstractions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Logging.Abstractions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.ObjectPool" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Options" Version="10.0.0" />

    <!-- EF Core -->
    <PackageVersion Include="Microsoft.EntityFrameworkCore" Version="10.0.0" />
    <PackageVersion Include="Microsoft.EntityFrameworkCore.Relational" Version="10.0.0" />
    <PackageVersion Include="Npgsql.EntityFrameworkCore.PostgreSQL" Version="10.0.0" />

    <!-- Transports -->
    <PackageVersion Include="RabbitMQ.Client" Version="7.1.2" />
    <PackageVersion Include="Confluent.Kafka" Version="2.6.1" />
    <PackageVersion Include="NATS.Client.Core" Version="2.5.0" />
    <PackageVersion Include="StackExchange.Redis" Version="2.8.16" />

    <!-- Serialization -->
    <PackageVersion Include="MessagePack" Version="3.1.0" />

    <!-- OpenTelemetry -->
    <PackageVersion Include="OpenTelemetry.Api" Version="1.10.0" />

    <!-- Source Generators / Roslyn -->
    <PackageVersion Include="Microsoft.CodeAnalysis.CSharp" Version="4.12.0" />
    <PackageVersion Include="Microsoft.CodeAnalysis.Analyzers" Version="3.11.0" />

    <!-- Source Link -->
    <PackageVersion Include="Microsoft.SourceLink.GitHub" Version="8.0.0" />

    <!-- CLI -->
    <PackageVersion Include="System.CommandLine" Version="2.0.0-beta5" />
    <PackageVersion Include="Spectre.Console" Version="0.49.1" />

    <!-- Testing -->
    <PackageVersion Include="xunit" Version="2.9.2" />
    <PackageVersion Include="xunit.runner.visualstudio" Version="2.8.2" />
    <PackageVersion Include="Microsoft.NET.Test.Sdk" Version="17.12.0" />
    <PackageVersion Include="FluentAssertions" Version="6.12.2" />
    <PackageVersion Include="Microsoft.Extensions.TimeProvider.Testing" Version="10.0.0" />
    <PackageVersion Include="NSubstitute" Version="5.3.0" />
    <PackageVersion Include="Testcontainers.RabbitMq" Version="4.1.0" />
    <PackageVersion Include="Testcontainers.PostgreSql" Version="4.1.0" />
    <PackageVersion Include="Bogus" Version="35.6.1" />
    <PackageVersion Include="Verify.Xunit" Version="28.4.0" />

    <!-- Benchmarks -->
    <PackageVersion Include="BenchmarkDotNet" Version="0.14.0" />
  </ItemGroup>

</Project>
```

---

## src/AvtoBus.Core/AvtoBus.Core.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <Description>Core abstractions and in-memory transport for AvtoBus EDA framework.</Description>
    <IsAotCompatible>true</IsAotCompatible>
    <EnableTrimAnalyzer>true</EnableTrimAnalyzer>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.DependencyInjection" />
    <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.ObjectPool" />
    <PackageReference Include="OpenTelemetry.Api" />
  </ItemGroup>

  <ItemGroup>
    <!-- Подтягиваем Source Generator в потребляющие проекты автоматически -->
    <ProjectReference Include="..\AvtoBus.Generators\AvtoBus.Generators.csproj"
                      OutputItemType="Analyzer"
                      ReferenceOutputAssembly="false" />
  </ItemGroup>

</Project>
```

---

## src/AvtoBus/AvtoBus.csproj (метапакет)

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <Description>AvtoBus metapackage: Core + InMemory + JSON. Just add a transport.</Description>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="..\AvtoBus.Core\AvtoBus.Core.csproj" />
  </ItemGroup>

</Project>
```

---

## src/AvtoBus.Generators/AvtoBus.Generators.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <!-- Source Generators обязаны таргетить netstandard2.0 -->
    <TargetFramework>netstandard2.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>disable</ImplicitUsings>
    <IncludeBuildOutput>false</IncludeBuildOutput>
    <IsRoslynComponent>true</IsRoslynComponent>
    <EnforceExtendedAnalyzerRules>true</EnforceExtendedAnalyzerRules>
    <Description>Source generators for AvtoBus (dispatchers, JSON contexts, diagnostics).</Description>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp" PrivateAssets="all" />
    <PackageReference Include="Microsoft.CodeAnalysis.Analyzers" PrivateAssets="all" />
  </ItemGroup>

  <!-- Упаковываем генератор в analyzers/dotnet/cs -->
  <ItemGroup>
    <None Include="$(OutputPath)\$(AssemblyName).dll"
          Pack="true"
          PackagePath="analyzers/dotnet/cs"
          Visible="false" />
  </ItemGroup>

</Project>
```

---

## src/AvtoBus.RabbitMq/AvtoBus.RabbitMq.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <Description>RabbitMQ transport for AvtoBus (quorum queues, streams, confirms).</Description>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="..\AvtoBus.Core\AvtoBus.Core.csproj" />
    <PackageReference Include="RabbitMQ.Client" />
    <PackageReference Include="Microsoft.Extensions.ObjectPool" />
  </ItemGroup>

</Project>
```

---

## src/AvtoBus.Outbox.EfCore/AvtoBus.Outbox.EfCore.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <Description>Transactional outbox and inbox for AvtoBus using EF Core.</Description>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="..\AvtoBus.Core\AvtoBus.Core.csproj" />
    <PackageReference Include="Microsoft.EntityFrameworkCore" />
    <PackageReference Include="Microsoft.EntityFrameworkCore.Relational" />
  </ItemGroup>

</Project>
```

---

## AvtoBus.slnx (новый формат solution)

```xml
<Solution>
  <Folder Name="/src/">
    <Project Path="src/AvtoBus.Core/AvtoBus.Core.csproj" />
    <Project Path="src/AvtoBus/AvtoBus.csproj" />
    <Project Path="src/AvtoBus.RabbitMq/AvtoBus.RabbitMq.csproj" />
    <Project Path="src/AvtoBus.Kafka/AvtoBus.Kafka.csproj" />
    <Project Path="src/AvtoBus.Outbox.EfCore/AvtoBus.Outbox.EfCore.csproj" />
    <Project Path="src/AvtoBus.Sagas/AvtoBus.Sagas.csproj" />
    <Project Path="src/AvtoBus.Generators/AvtoBus.Generators.csproj" />
    <Project Path="src/AvtoBus.Testing/AvtoBus.Testing.csproj" />
    <Project Path="src/AvtoBus.Dashboard/AvtoBus.Dashboard.csproj" />
    <Project Path="src/AvtoBus.Cli/AvtoBus.Cli.csproj" />
  </Folder>
  <Folder Name="/tests/">
    <Project Path="tests/AvtoBus.Core.Tests/AvtoBus.Core.Tests.csproj" />
    <Project Path="tests/AvtoBus.Sagas.Tests/AvtoBus.Sagas.Tests.csproj" />
    <Project Path="tests/AvtoBus.Integration.Tests/AvtoBus.Integration.Tests.csproj" />
  </Folder>
  <Folder Name="/samples/">
    <Project Path="samples/ECommerce/Orders/Orders.csproj" />
  </Folder>
</Solution>
```

---

## .editorconfig (ключевые правила)

```ini
root = true

[*.cs]
indent_style = space
indent_size = 4
end_of_line = lf
charset = utf-8
trim_trailing_whitespace = true
insert_final_newline = true

# Namespaces
csharp_style_namespace_declarations = file_scoped:error

# var
csharp_style_var_when_type_is_apparent = true:suggestion

# Expression-bodied
csharp_style_expression_bodied_methods = when_on_single_line:suggestion

# Nullable
dotnet_diagnostic.CS8600.severity = error
dotnet_diagnostic.CS8602.severity = error
dotnet_diagnostic.CS8618.severity = error

# AvtoBus analyzers
dotnet_diagnostic.AVB001.severity = error
dotnet_diagnostic.AVB002.severity = error
dotnet_diagnostic.AVB003.severity = warning
dotnet_diagnostic.AVB010.severity = warning
```
