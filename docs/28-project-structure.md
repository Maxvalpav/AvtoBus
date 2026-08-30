# 🏗 Структура репозитория, .csproj и CI/CD

> **Статус: Design draft.** Дерево каталогов и workflow — предложение для первого PR, а не описание существующего репозитория.

Всё, что нужно, чтобы код из `docs/code/*` превратился в реальный солюшен.

---

## 1. Дерево репозитория

```
avtobus/
├── .github/
│   └── workflows/
│       ├── ci.yml                    # build + test + analyzers
│       ├── release.yml               # pack + push NuGet
│       ├── benchmarks.yml            # perf-гейт на PR
│       └── codeql.yml                # security scanning
├── src/
│   ├── AvtoBus.Core/                 # ядро (без зависимостей от брокеров)
│   ├── AvtoBus/                      # метапакет: Core + InMemory + Json
│   ├── AvtoBus.Generators/           # Source Generators (netstandard2.0!)
│   ├── AvtoBus.Analyzers/            # Roslyn analyzers + code fixes
│   ├── AvtoBus.RabbitMq/
│   ├── AvtoBus.Kafka/
│   ├── AvtoBus.AzureServiceBus/
│   ├── AvtoBus.Nats/
│   ├── AvtoBus.Redis/
│   ├── AvtoBus.Sql/                  # SQL-транспорт
│   ├── AvtoBus.Outbox.EfCore/
│   ├── AvtoBus.Outbox.Dapper/
│   ├── AvtoBus.Sagas/
│   ├── AvtoBus.Scheduling/
│   ├── AvtoBus.EventSourcing/
│   ├── AvtoBus.Serialization.MessagePack/
│   ├── AvtoBus.Serialization.Protobuf/
│   ├── AvtoBus.Security/             # подпись, шифрование, PII
│   ├── AvtoBus.MultiTenancy/
│   ├── AvtoBus.Dashboard/            # Blazor UI
│   ├── AvtoBus.Testing/
│   └── AvtoBus.Cli/                  # dotnet tool
├── tests/
│   ├── AvtoBus.Core.Tests/
│   ├── AvtoBus.Generators.Tests/     # snapshot-тесты генерации
│   ├── AvtoBus.Analyzers.Tests/
│   ├── AvtoBus.Conformance/          # общий kit для транспортов
│   ├── AvtoBus.RabbitMq.Tests/       # Testcontainers
│   ├── AvtoBus.Kafka.Tests/
│   ├── AvtoBus.Outbox.Tests/
│   ├── AvtoBus.Sagas.Tests/
│   ├── AvtoBus.EventSourcing.Tests/
│   └── AvtoBus.Integration.Tests/    # end-to-end
├── benchmarks/
│   └── AvtoBus.Benchmarks/
├── samples/
│   ├── AvtoBus.QuickStart/           # ASP.NET Core + RabbitMQ + outbox + dashboard
│   ├── AvtoBus.AotSample/            # Native AOT worker (InMemory)
│   ├── AvtoBus.AotSample.RabbitMq/   # Native AOT worker (RabbitMQ)
│   ├── AvtoBus.Logistics/            # 30 логистических микросервисов, отдельное решение (идея 27)
│   │   ├── Contracts/                # контракт-пакет (5 неймспейсов)
│   │   ├── services/                 # 30 сервисов по принципу «один сервис — один csproj»
│   │   └── runner/                   # оркестратор: все 30 сервисов в одном процессе на InMemory
│   ├── 01-hello-world/
│   ├── 02-outbox/
│   ├── 03-saga/
│   ├── 04-event-sourcing/
│   ├── 05-modular-monolith/
│   └── ecommerce/                    # эталонный пример (6 сервисов)
├── templates/
│   └── AvtoBus.Templates/            # dotnet new
├── docs/                             # эта документация
├── build/
│   ├── docker-compose.dev.yml
│   └── scripts/
├── Directory.Build.props
├── Directory.Packages.props
├── .editorconfig
├── global.json
├── nuget.config
├── AvtoBus.sln
├── README.md
├── LICENSE
├── SECURITY.md
├── CONTRIBUTING.md
└── CHANGELOG.md
```

---

## 2. global.json

```json
{
  "sdk": {
    "version": "10.0.100",
    "rollForward": "latestFeature"
  }
}
```

---

## 3. Directory.Build.props

```xml
<Project>

  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <LangVersion>preview</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <WarningsNotAsErrors>NU1901;NU1902;NU1903</WarningsNotAsErrors>
    <EnforceCodeStyleInBuild>true</EnforceCodeStyleInBuild>
    <AnalysisLevel>latest-recommended</AnalysisLevel>
    <EnableNETAnalyzers>true</EnableNETAnalyzers>
    <GenerateDocumentationFile>true</GenerateDocumentationFile>
    <NoWarn>$(NoWarn);CS1591</NoWarn>
    <InvariantGlobalization>true</InvariantGlobalization>
    <IsAotCompatible>true</IsAotCompatible>
    <EnableTrimAnalyzer>true</EnableTrimAnalyzer>
    <PublishAot Condition="'$(Configuration)' == 'Release'">false</PublishAot>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
  </PropertyGroup>

  <!-- NuGet-метаданные для всех src/* проектов -->
  <PropertyGroup Condition="$(MSBuildProjectDirectory.Contains('src'))">
    <IsPackable>true</IsPackable>
    <Authors>AvtoBus Contributors</Authors>
    <Company>AvtoBus</Company>
    <Product>AvtoBus</Product>
    <Description>AvtoBus — современный EDA-фреймворк для ASP.NET Core. Простой как Wolverine, надёжный как NServiceBus.</Description>
    <PackageTags>eda;messaging;event-driven;cqrs;saga;outbox;rabbitmq;kafka;event-sourcing;aspnetcore</PackageTags>
    <PackageProjectUrl>https://github.com/avtobus/avtobus</PackageProjectUrl>
    <RepositoryUrl>https://github.com/avtobus/avtobus</RepositoryUrl>
    <RepositoryType>git</RepositoryType>
    <PackageLicenseExpression>MIT</PackageLicenseExpression>
    <PackageReadmeFile>README.md</PackageReadmeFile>
    <PackageIcon>icon.png</PackageIcon>
    <PublishRepositoryUrl>true</PublishRepositoryUrl>
    <EmbedUntrackedSources>true</EmbedUntrackedSources>
    <IncludeSymbols>true</IncludeSymbols>
    <SymbolPackageFormat>snupkg</SymbolPackageFormat>
    <ContinuousIntegrationBuild Condition="'$(GITHUB_ACTIONS)' == 'true'">true</ContinuousIntegrationBuild>
    <Deterministic>true</Deterministic>
  </PropertyGroup>

  <ItemGroup Condition="'$(IsPackable)' == 'true'">
    <None Include="$(MSBuildThisFileDirectory)README.md" Pack="true" PackagePath="\" Visible="false" />
    <None Include="$(MSBuildThisFileDirectory)build/icon.png" Pack="true" PackagePath="\" Visible="false" />
  </ItemGroup>

  <!-- Тестовые проекты -->
  <PropertyGroup Condition="$(MSBuildProjectName.EndsWith('.Tests'))">
    <IsPackable>false</IsPackable>
    <IsTestProject>true</IsTestProject>
    <TreatWarningsAsErrors>false</TreatWarningsAsErrors>
  </PropertyGroup>

  <ItemGroup Condition="'$(IsTestProject)' == 'true'">
    <PackageReference Include="Microsoft.NET.Test.Sdk" />
    <PackageReference Include="xunit.v3" />
    <PackageReference Include="xunit.runner.visualstudio" />
    <PackageReference Include="Shouldly" />
    <PackageReference Include="NSubstitute" />
    <PackageReference Include="Microsoft.Extensions.TimeProvider.Testing" />
    <PackageReference Include="coverlet.collector" />
    <Using Include="Xunit" />
    <Using Include="Shouldly" />
  </ItemGroup>

  <!-- Source Link -->
  <ItemGroup>
    <PackageReference Include="Microsoft.SourceLink.GitHub" PrivateAssets="All" />
  </ItemGroup>

</Project>
```

---

## 4. Directory.Packages.props (Central Package Management)

```xml
<Project>
  <PropertyGroup>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
    <CentralPackageTransitivePinningEnabled>true</CentralPackageTransitivePinningEnabled>
  </PropertyGroup>

  <ItemGroup Label="Microsoft.Extensions">
    <PackageVersion Include="Microsoft.Extensions.DependencyInjection.Abstractions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Hosting.Abstractions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Logging.Abstractions" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Options" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Diagnostics.HealthChecks" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.ObjectPool" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.TimeProvider.Testing" Version="10.0.0" />
  </ItemGroup>

  <ItemGroup Label="Data">
    <PackageVersion Include="Microsoft.EntityFrameworkCore" Version="10.0.0" />
    <PackageVersion Include="Microsoft.EntityFrameworkCore.Relational" Version="10.0.0" />
    <PackageVersion Include="Npgsql" Version="9.0.2" />
    <PackageVersion Include="Npgsql.EntityFrameworkCore.PostgreSQL" Version="10.0.0" />
    <PackageVersion Include="Dapper" Version="2.1.66" />
  </ItemGroup>

  <ItemGroup Label="Transports">
    <PackageVersion Include="RabbitMQ.Client" Version="7.1.2" />
    <PackageVersion Include="Confluent.Kafka" Version="2.8.0" />
    <PackageVersion Include="Azure.Messaging.ServiceBus" Version="7.18.4" />
    <PackageVersion Include="NATS.Client.Core" Version="2.5.9" />
    <PackageVersion Include="NATS.Client.JetStream" Version="2.5.9" />
    <PackageVersion Include="StackExchange.Redis" Version="2.8.24" />
    <PackageVersion Include="AWSSDK.SQS" Version="3.7.400" />
  </ItemGroup>

  <ItemGroup Label="Serialization">
    <PackageVersion Include="MessagePack" Version="3.1.8" />
    <PackageVersion Include="Google.Protobuf" Version="3.29.3" />
    <PackageVersion Include="MemoryPack" Version="1.21.3" />
  </ItemGroup>

  <ItemGroup Label="Observability">
    <PackageVersion Include="OpenTelemetry" Version="1.11.1" />
    <PackageVersion Include="OpenTelemetry.Extensions.Hosting" Version="1.11.1" />
    <PackageVersion Include="System.Diagnostics.DiagnosticSource" Version="10.0.0" />
  </ItemGroup>

  <ItemGroup Label="Roslyn">
    <PackageVersion Include="Microsoft.CodeAnalysis.CSharp" Version="4.12.0" />
    <PackageVersion Include="Microsoft.CodeAnalysis.Analyzers" Version="3.11.0" />
    <PackageVersion Include="Microsoft.CodeAnalysis.CSharp.Workspaces" Version="4.12.0" />
    <PackageVersion Include="Microsoft.CodeAnalysis.CSharp.Analyzer.Testing" Version="1.1.2" />
  </ItemGroup>

  <ItemGroup Label="CLI">
    <PackageVersion Include="System.CommandLine" Version="2.0.0-beta5.25057.1" />
    <PackageVersion Include="Spectre.Console" Version="0.49.1" />
    <PackageVersion Include="Spectre.Console.Cli" Version="0.49.1" />
  </ItemGroup>

  <ItemGroup Label="Testing">
    <PackageVersion Include="Microsoft.NET.Test.Sdk" Version="17.12.0" />
    <PackageVersion Include="xunit.v3" Version="1.0.1" />
    <PackageVersion Include="xunit.runner.visualstudio" Version="3.0.1" />
    <PackageVersion Include="Shouldly" Version="4.3.0" />
    <PackageVersion Include="NSubstitute" Version="5.3.0" />
    <PackageVersion Include="Testcontainers" Version="4.1.0" />
    <PackageVersion Include="Testcontainers.RabbitMq" Version="4.1.0" />
    <PackageVersion Include="Testcontainers.Kafka" Version="4.1.0" />
    <PackageVersion Include="Testcontainers.PostgreSql" Version="4.1.0" />
    <PackageVersion Include="Verify.Xunit" Version="28.7.0" />
    <PackageVersion Include="Verify.SourceGenerators" Version="2.5.0" />
    <PackageVersion Include="coverlet.collector" Version="6.0.4" />
    <PackageVersion Include="BenchmarkDotNet" Version="0.14.0" />
  </ItemGroup>

  <ItemGroup Label="Build">
    <PackageVersion Include="Microsoft.SourceLink.GitHub" Version="8.0.0" />
    <PackageVersion Include="Polly.Core" Version="8.5.0" />
  </ItemGroup>
</Project>
```

---

## 5. Ключевые .csproj

### src/AvtoBus.Core/AvtoBus.Core.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <PackageId>AvtoBus.Core</PackageId>
    <Description>Ядро AvtoBus: абстракции, конверт сообщения, пайплайн middleware.</Description>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.Diagnostics.HealthChecks" />
    <PackageReference Include="System.Diagnostics.DiagnosticSource" />
  </ItemGroup>

  <ItemGroup>
    <InternalsVisibleTo Include="AvtoBus" />
    <InternalsVisibleTo Include="AvtoBus.Testing" />
    <InternalsVisibleTo Include="AvtoBus.Core.Tests" />
    <InternalsVisibleTo Include="DynamicProxyGenAssembly2" />
  </ItemGroup>

</Project>
```

### src/AvtoBus/AvtoBus.csproj (метапакет)

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <PackageId>AvtoBus</PackageId>
    <Description>AvtoBus — метапакет: Core + InMemory транспорт + JSON + Source Generators. Начни отсюда.</Description>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="../AvtoBus.Core/AvtoBus.Core.csproj" />
  </ItemGroup>

  <!-- Source Generator и Analyzers упаковываются внутрь пакета -->
  <ItemGroup>
    <ProjectReference Include="../AvtoBus.Generators/AvtoBus.Generators.csproj"
                      OutputItemType="Analyzer"
                      ReferenceOutputAssembly="false"
                      PrivateAssets="all" />
    <ProjectReference Include="../AvtoBus.Analyzers/AvtoBus.Analyzers.csproj"
                      OutputItemType="Analyzer"
                      ReferenceOutputAssembly="false"
                      PrivateAssets="all" />
  </ItemGroup>

  <ItemGroup>
    <None Include="$(OutputPath)/../../../AvtoBus.Generators/$(Configuration)/netstandard2.0/AvtoBus.Generators.dll"
          Pack="true" PackagePath="analyzers/dotnet/cs" Visible="false" />
    <None Include="$(OutputPath)/../../../AvtoBus.Analyzers/$(Configuration)/netstandard2.0/AvtoBus.Analyzers.dll"
          Pack="true" PackagePath="analyzers/dotnet/cs" Visible="false" />
  </ItemGroup>

</Project>
```

### src/AvtoBus.Generators/AvtoBus.Generators.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <!-- ВАЖНО: генераторы обязаны быть netstandard2.0 -->
    <TargetFramework>netstandard2.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>false</ImplicitUsings>
    <IsPackable>false</IsPackable>
    <EnforceExtendedAnalyzerRules>true</EnforceExtendedAnalyzerRules>
    <IncludeBuildOutput>false</IncludeBuildOutput>
    <IsRoslynComponent>true</IsRoslynComponent>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp" PrivateAssets="all" />
    <PackageReference Include="Microsoft.CodeAnalysis.Analyzers" PrivateAssets="all" />
  </ItemGroup>

  <ItemGroup>
    <!-- Полифилы для netstandard2.0 -->
    <Compile Include="../../build/polyfills/*.cs" Link="Polyfills/%(Filename)%(Extension)" />
  </ItemGroup>

</Project>
```

### src/AvtoBus.RabbitMq/AvtoBus.RabbitMq.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <PackageId>AvtoBus.RabbitMq</PackageId>
    <Description>Транспорт RabbitMQ для AvtoBus: quorum queues, streams, publisher confirms.</Description>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="../AvtoBus.Core/AvtoBus.Core.csproj" />
  </ItemGroup>

  <ItemGroup>
    <PackageReference Include="RabbitMQ.Client" />
    <PackageReference Include="Microsoft.Extensions.ObjectPool" />
  </ItemGroup>

</Project>
```

### src/AvtoBus.Cli/AvtoBus.Cli.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <PackAsTool>true</PackAsTool>
    <ToolCommandName>avtobus</ToolCommandName>
    <PackageId>AvtoBus.Cli</PackageId>
    <Description>CLI-инструмент AvtoBus: topology, dlq, saga, projections, doctor, dev.</Description>
    <PublishAot>true</PublishAot>
    <InvariantGlobalization>true</InvariantGlobalization>
  </PropertyGroup>

  <ItemGroup>
    <ProjectReference Include="../AvtoBus.Core/AvtoBus.Core.csproj" />
    <ProjectReference Include="../AvtoBus.RabbitMq/AvtoBus.RabbitMq.csproj" />
  </ItemGroup>

  <ItemGroup>
    <PackageReference Include="System.CommandLine" />
    <PackageReference Include="Spectre.Console.Cli" />
  </ItemGroup>

</Project>
```

---

## 6. .editorconfig (ключевые правила)

```ini
root = true

[*]
charset = utf-8
end_of_line = lf
insert_final_newline = true
trim_trailing_whitespace = true
indent_style = space

[*.cs]
indent_size = 4
max_line_length = 120

# Стиль
csharp_style_namespace_declarations = file_scoped:error
csharp_style_var_for_built_in_types = true:suggestion
csharp_style_expression_bodied_methods = when_on_single_line:suggestion
csharp_prefer_braces = when_multiline:suggestion
dotnet_style_require_accessibility_modifiers = always:error
dotnet_style_readonly_field = true:warning

# Производительность (важно для шины!)
dotnet_diagnostic.CA1848.severity = warning   # LoggerMessage delegates
dotnet_diagnostic.CA1849.severity = error     # Call async methods in async context
dotnet_diagnostic.CA2007.severity = none      # ConfigureAwait не нужен в хосте
dotnet_diagnostic.CA1062.severity = none      # Nullable вместо проверок

# AOT / Trimming
dotnet_diagnostic.IL2026.severity = error
dotnet_diagnostic.IL3050.severity = error

# AvtoBus-специфичные (наши анализаторы)
dotnet_diagnostic.AVB001.severity = error
dotnet_diagnostic.AVB002.severity = error
dotnet_diagnostic.AVB003.severity = warning
dotnet_diagnostic.AVB010.severity = warning
dotnet_diagnostic.AVB017.severity = error

[*.{json,yml,yaml}]
indent_size = 2

[*.{csproj,props,targets}]
indent_size = 2
```

---

## 7. .github/workflows/ci.yml

```yaml
name: CI

on:
  push:
    branches: [main, 'release/*']
  pull_request:
    branches: [main]

env:
  DOTNET_NOLOGO: true
  DOTNET_SKIP_FIRST_TIME_EXPERIENCE: true
  DOTNET_CLI_TELEMETRY_OPTOUT: true

jobs:
  build:
    name: Build & Unit Tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }

      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'

      - name: Restore
        run: dotnet restore

      - name: Build
        run: dotnet build --no-restore -c Release /warnaserror

      - name: Unit tests
        run: >
          dotnet test --no-build -c Release
          --filter "Category!=Integration"
          --collect:"XPlat Code Coverage"
          --logger "trx;LogFileName=test-results.trx"

      - name: Upload coverage
        uses: codecov/codecov-action@v5
        with:
          files: '**/coverage.cobertura.xml'

  integration:
    name: Integration Tests (Testcontainers)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with: { dotnet-version: '10.0.x' }

      - name: Integration tests
        run: dotnet test -c Release --filter "Category=Integration"
        env:
          TESTCONTAINERS_RYUK_DISABLED: false

  conformance:
    name: Transport Conformance Kit
    runs-on: ubuntu-latest
    strategy:
      matrix:
        transport: [inmemory, rabbitmq, kafka, redis, sql]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with: { dotnet-version: '10.0.x' }

      - name: Conformance ${{ matrix.transport }}
        run: dotnet test tests/AvtoBus.Conformance -c Release
        env:
          AVTOBUS_TRANSPORT: ${{ matrix.transport }}

  aot:
    name: Native AOT Publish
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with: { dotnet-version: '10.0.x' }

      - name: AOT publish sample
        run: |
          dotnet publish samples/01-hello-world -c Release -r linux-x64 \
            /p:PublishAot=true /warnaserror

      - name: Check binary size & startup
        run: |
          SIZE=$(stat -c%s samples/01-hello-world/bin/Release/net10.0/linux-x64/publish/hello-world)
          echo "Binary size: $((SIZE/1024/1024)) MB"
          test $SIZE -lt 41943040 || (echo "Binary too large (>40MB)" && exit 1)

  analyzers:
    name: Analyzer Tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with: { dotnet-version: '10.0.x' }
      - run: dotnet test tests/AvtoBus.Analyzers.Tests -c Release
      - run: dotnet test tests/AvtoBus.Generators.Tests -c Release

  docs:
    name: Doc-tests (snippets compile)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with: { dotnet-version: '10.0.x' }
      - name: Extract & compile doc snippets
        run: dotnet run --project build/DocTests -- docs/
```

---

## 8. .github/workflows/benchmarks.yml (perf-гейт)

```yaml
name: Benchmarks

on:
  pull_request:
    paths: ['src/**']

jobs:
  bench:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }

      - uses: actions/setup-dotnet@v4
        with: { dotnet-version: '10.0.x' }

      - name: Run benchmarks (PR)
        run: >
          dotnet run -c Release --project benchmarks/AvtoBus.Benchmarks --
          --filter '*Publish*|*Consume*' --exporters json --artifacts ./bench-pr

      - name: Checkout baseline
        run: git checkout ${{ github.base_ref }}

      - name: Run benchmarks (baseline)
        run: >
          dotnet run -c Release --project benchmarks/AvtoBus.Benchmarks --
          --filter '*Publish*|*Consume*' --exporters json --artifacts ./bench-base

      - name: Compare & gate
        run: |
          dotnet run --project build/BenchCompare -- \
            --baseline ./bench-base --current ./bench-pr \
            --max-throughput-regression 5 \
            --max-allocation-regression 10

      - name: Comment results on PR
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            const body = fs.readFileSync('bench-report.md', 'utf8');
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body
            });
```

---

## 9. .github/workflows/release.yml

```yaml
name: Release

on:
  push:
    tags: ['v*']

permissions:
  contents: write
  id-token: write        # для NuGet trusted publishing

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }

      - uses: actions/setup-dotnet@v4
        with: { dotnet-version: '10.0.x' }

      - name: Version from tag
        run: echo "VERSION=${GITHUB_REF_NAME#v}" >> $GITHUB_ENV

      - name: Pack
        run: dotnet pack -c Release /p:Version=$VERSION -o ./artifacts

      - name: Generate SBOM
        run: |
          dotnet tool install --global Microsoft.Sbom.DotNetTool
          sbom-tool generate -b ./artifacts -bc . -pn AvtoBus -pv $VERSION -ps AvtoBus

      - name: Push to NuGet
        run: dotnet nuget push ./artifacts/*.nupkg --source https://api.nuget.org/v3/index.json --skip-duplicate
        env:
          NUGET_AUTH_TOKEN: ${{ secrets.NUGET_API_KEY }}

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          files: ./artifacts/*
          generate_release_notes: true
          body_path: CHANGELOG.md
```

---

## 10. build/docker-compose.dev.yml

```yaml
name: avtobus-dev

services:
  rabbitmq:
    image: rabbitmq:4-management-alpine
    ports: ["5672:5672", "15672:15672"]
    environment:
      RABBITMQ_DEFAULT_USER: avtobus
      RABBITMQ_DEFAULT_PASS: avtobus
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "ping"]
      interval: 10s
      retries: 5

  postgres:
    image: postgres:17-alpine
    ports: ["5432:5432"]
    environment:
      POSTGRES_USER: avtobus
      POSTGRES_PASSWORD: avtobus
      POSTGRES_DB: avtobus
    volumes:
      - ./sql:/docker-entrypoint-initdb.d:ro
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U avtobus"]
      interval: 5s
      retries: 5

  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda start
      - --smp 1
      - --overprovisioned
      - --kafka-addr PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr PLAINTEXT://localhost:9092
    ports: ["9092:9092", "9644:9644"]

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  jaeger:
    image: jaegertracing/all-in-one:latest
    ports: ["16686:16686", "4317:4317", "4318:4318"]
    environment:
      COLLECTOR_OTLP_ENABLED: true

  grafana:
    image: grafana/grafana-oss:latest
    ports: ["3000:3000"]
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: true
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    volumes:
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards:ro

volumes:
  pgdata:
```

---

## 11. AvtoBus.sln (генерация)

```bash
dotnet new sln -n AvtoBus

# src
for p in Core Generators Analyzers RabbitMq Kafka AzureServiceBus Nats Redis Sql \
         Outbox.EfCore Outbox.Dapper Sagas Scheduling EventSourcing \
         Serialization.MessagePack Serialization.Protobuf Security MultiTenancy \
         Dashboard Testing Cli; do
  dotnet sln add "src/AvtoBus.$p/AvtoBus.$p.csproj" --solution-folder src
done
dotnet sln add src/AvtoBus/AvtoBus.csproj --solution-folder src

# tests
dotnet sln add tests/**/*.csproj --solution-folder tests

# benchmarks / samples
dotnet sln add benchmarks/**/*.csproj --solution-folder benchmarks
dotnet sln add samples/**/*.csproj --solution-folder samples
```

---

## 12. nuget.config

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" protocolVersion="3" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="nuget.org">
      <package pattern="*" />
    </packageSource>
  </packageSourceMapping>
  <config>
    <add key="signatureValidationMode" value="require" />
  </config>
</configuration>
```

---

## 13. Матрица зависимостей пакетов

```
AvtoBus.Core  ← ничего кроме Microsoft.Extensions.*
   ↑
   ├── AvtoBus (метапакет + Generators + Analyzers)
   ├── AvtoBus.RabbitMq       → RabbitMQ.Client
   ├── AvtoBus.Kafka          → Confluent.Kafka
   ├── AvtoBus.Nats           → NATS.Client
   ├── AvtoBus.Redis          → StackExchange.Redis
   ├── AvtoBus.Sql            → Npgsql / Microsoft.Data.SqlClient
   ├── AvtoBus.Outbox.EfCore  → EFCore.Relational
   ├── AvtoBus.Sagas          → (Core только)
   ├── AvtoBus.Scheduling     → Npgsql
   ├── AvtoBus.EventSourcing  → Npgsql
   ├── AvtoBus.Security       → System.Security.Cryptography
   ├── AvtoBus.Dashboard      → AspNetCore + Blazor
   ├── AvtoBus.Testing        → Core + InMemory + TimeProvider.Testing
   └── AvtoBus.Cli            → Spectre.Console + System.CommandLine

Правило: Core НИКОГДА не зависит от брокеров, БД или ASP.NET.
```
