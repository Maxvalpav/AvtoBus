# CI/CD, упаковка NuGet, релизы

---

## .github/workflows/ci.yml

```yaml
name: CI

on:
  push:
    branches: [main, 'release/**']
  pull_request:
    branches: [main]

env:
  DOTNET_NOLOGO: true
  DOTNET_CLI_TELEMETRY_OPTOUT: true
  DOTNET_SKIP_FIRST_TIME_EXPERIENCE: true

jobs:
  build-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # для GitVersion/SourceLink

      - name: Setup .NET
        uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'

      - name: Restore
        run: dotnet restore

      - name: Build
        run: dotnet build --no-restore -c Release

      - name: Unit tests
        run: >
          dotnet test --no-build -c Release
          --filter "Category!=Integration"
          --logger "trx;LogFileName=unit.trx"
          --collect:"XPlat Code Coverage"

      - name: Integration tests (Testcontainers)
        run: >
          dotnet test --no-build -c Release
          --filter "Category=Integration"
          --logger "trx;LogFileName=integration.trx"

      - name: Upload coverage
        uses: codecov/codecov-action@v4
        with:
          token: ${{ secrets.CODECOV_TOKEN }}

      - name: Publish test results
        uses: dorny/test-reporter@v1
        if: always()
        with:
          name: Test Results
          path: '**/*.trx'
          reporter: dotnet-trx

  aot-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'

      - name: Install AOT prerequisites
        run: sudo apt-get install -y clang zlib1g-dev

      - name: AOT publish sample
        run: >
          dotnet publish samples/QuickStart/QuickStart.csproj
          -c Release -r linux-x64 --aot
          /p:PublishAot=true

      - name: Verify no AOT warnings
        run: |
          if grep -r "AOT analysis warning" publish.log; then
            echo "AOT warnings found!"
            exit 1
          fi

  schema-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'

      - name: Install CLI
        run: dotnet tool install -g AvtoBus.Cli --add-source ./artifacts

      - name: Check contract compatibility
        run: avtobus schema check --against origin/main
```

---

## .github/workflows/perf.yml

```yaml
name: Performance

on:
  pull_request:
    paths:
      - 'src/**'
      - 'benchmarks/**'

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'

      - name: Run benchmarks
        run: >
          dotnet run -c Release
          --project benchmarks/AvtoBus.Benchmarks
          -- --filter '*Publish*' --exporters json

      - name: Compare against baseline
        run: |
          pwsh ./scripts/compare-bench.ps1 \
            --current ./BenchmarkDotNet.Artifacts/results/*.json \
            --baseline ./benchmarks/baseline.json \
            --throughput-tolerance 5 \
            --allocation-tolerance 10

      - name: Comment PR with results
        uses: actions/github-script@v7
        with:
          script: |
            const results = require('./bench-summary.json');
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: results.markdown
            });
```

---

## .github/workflows/release.yml

```yaml
name: Release

on:
  push:
    tags:
      - 'v*'

permissions:
  contents: write
  packages: write
  id-token: write   # для OIDC / sigstore

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - uses: actions/setup-dotnet@v4
        with:
          dotnet-version: '10.0.x'

      - name: Derive version from tag
        run: echo "VERSION=${GITHUB_REF#refs/tags/v}" >> $GITHUB_ENV

      - name: Build
        run: dotnet build -c Release /p:Version=${{ env.VERSION }}

      - name: Test
        run: dotnet test -c Release --no-build --filter "Category!=Integration"

      - name: Pack
        run: >
          dotnet pack -c Release --no-build
          /p:Version=${{ env.VERSION }}
          -o ./artifacts

      - name: Generate SBOM
        run: |
          dotnet tool install -g Microsoft.Sbom.DotNetTool
          sbom-tool generate -b ./artifacts -bc . -pn AvtoBus -pv ${{ env.VERSION }} -ps "AvtoBus"

      - name: Sign packages
        run: |
          dotnet nuget sign ./artifacts/*.nupkg \
            --certificate-fingerprint ${{ secrets.CERT_FINGERPRINT }} \
            --timestamper http://timestamp.digicert.com

      - name: Push to NuGet
        run: >
          dotnet nuget push ./artifacts/*.nupkg
          --source https://api.nuget.org/v3/index.json
          --api-key ${{ secrets.NUGET_API_KEY }}
          --skip-duplicate

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          files: ./artifacts/*.nupkg
          generate_release_notes: true
          body_path: ./CHANGELOG-latest.md
```

---

## scripts/compare-bench.ps1

```powershell
param(
    [string]$Current,
    [string]$Baseline,
    [double]$ThroughputTolerance = 5,
    [double]$AllocationTolerance = 10
)

$current = Get-Content $Current | ConvertFrom-Json
$baseline = Get-Content $Baseline | ConvertFrom-Json

$failed = $false
$summary = @("| Benchmark | Baseline | Current | Delta | Status |", "|---|---|---|---|---|")

foreach ($bench in $current.Benchmarks) {
    $base = $baseline.Benchmarks | Where-Object { $_.FullName -eq $bench.FullName }
    if (-not $base) { continue }

    $throughputDelta = (($bench.Statistics.Mean - $base.Statistics.Mean) / $base.Statistics.Mean) * 100
    $allocDelta = (($bench.Memory.BytesAllocatedPerOperation - $base.Memory.BytesAllocatedPerOperation) / $base.Memory.BytesAllocatedPerOperation) * 100

    $status = "✅"
    if ($throughputDelta -gt $ThroughputTolerance) {
        $status = "❌ SLOWER"
        $failed = $true
    }
    if ($allocDelta -gt $AllocationTolerance) {
        $status = "❌ MORE ALLOC"
        $failed = $true
    }

    $summary += "| $($bench.DisplayInfo) | $($base.Statistics.Mean)ns | $($bench.Statistics.Mean)ns | $([math]::Round($throughputDelta,1))% | $status |"
}

$md = $summary -join "`n"
@{ markdown = "## Benchmark Results`n`n$md" } | ConvertTo-Json | Out-File bench-summary.json

if ($failed) {
    Write-Error "Performance regression detected!"
    exit 1
}
```

---

## SECURITY.md

```markdown
# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x LTS | ✅ (until 2028)    |
| 0.x     | ⚠️ pre-release     |

## Reporting a Vulnerability

Please **do not** open public issues for security vulnerabilities.

We aim to:
- Acknowledge within **48 hours**
- Provide assessment within **7 days**
- Release a fix within **30 days** for critical issues

## Threat Model

See [threat model](../36-threat-model.md) for the full STRIDE analysis.

Covered by the framework:
- ✅ Message tampering (HMAC/Ed25519 signatures)
- ✅ Eavesdropping (envelope encryption, mTLS)
- ✅ Deserialization attacks (allowlist-only type resolution)
- ✅ Replay attacks (inbox deduplication)
- ✅ Injection (payload validation, quarantine)

Your responsibility:
- Transport-level network security (firewall, VPC)
- Secret management (use ISecretProvider)
- Broker access control (RBAC)
```

---

## CHANGELOG.md (Keep a Changelog format)

```markdown
# Changelog

All notable changes to AvtoBus are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added
- Core bus abstractions (IBus, ConsumeContext, Envelope)
- InMemory transport with full broker semantics
- RabbitMQ transport (quorum queues, publisher confirms)
- Transactional Outbox/Inbox for EF Core
- Sagas (state-based + durable execution)
- Source Generator for zero-reflection dispatch
- Test harness with virtual time
- Dashboard (Blazor)
- CLI (avtobus)

### Security
- Allowlist-only type resolution (no arbitrary deserialization)

## [0.1.0] - 2026-XX-XX

Initial preview release.
```

---

## nuget.config

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>

  <packageSourceMapping>
    <packageSource key="nuget.org">
      <package pattern="*" />
    </packageSource>
  </packageSourceMapping>

  <!-- Проверка подписей пакетов -->
  <config>
    <add key="signatureValidationMode" value="require" />
  </config>
</configuration>
```

---

## Dockerfile (для samples и воркеров)

```dockerfile
# ── Build ──
FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build
WORKDIR /src

COPY Directory.Build.props Directory.Packages.props global.json nuget.config ./
COPY src/ ./src/
COPY samples/ ./samples/

RUN dotnet restore samples/ECommerce/Orders/Orders.csproj
RUN dotnet publish samples/ECommerce/Orders/Orders.csproj \
    -c Release -o /app --no-restore

# ── Runtime (chiseled, non-root) ──
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS runtime
WORKDIR /app
COPY --from=build /app .

USER $APP_UID
EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD ["/app/Orders", "--healthcheck"]

ENTRYPOINT ["/app/Orders"]
```

---

## Что осталось за рамками (roadmap-задачи с приоритетами)

| Задача | Приоритет | Оценка | Статус |
|--------|-----------|--------|--------|
| Serializer + маркеры + каталог | P0 | 3 дня | ✅ описано (файл 11) |
| Структура solution + .csproj | P0 | 2 дня | ✅ описано (файл 12) |
| Юнит-тесты ядра | P0 | 5 дней | ✅ описано (файл 13) |
| CI/CD + packaging | P0 | 3 дня | ✅ описано (файл 14) |
| **Kafka transport (полный)** | P1 | 5 дней | ⏳ скетч в 18 |
| **Scheduling (cron)** | P1 | 4 дня | ⏳ идеи 223–228 |
| **Event Sourcing (код)** | P1 | 10 дней | ⏳ идеи 251–300 |
| **Analyzers + code-fixes** | P1 | 5 дней | ⏳ дизайн в 16 |
| **Dashboard SPA (Blazor UI)** | P2 | 8 дней | ⏳ API в 23 |
| **CLI (полная реализация)** | P2 | 6 дней | ⏳ дизайн в 25 |
| NATS/Redis/SQS транспорты | P2 | 12 дней | ⏳ идеи 63–69 |
| AsyncAPI генератор | P2 | 4 дня | ⏳ идея 114 |
| Multi-region | P3 | 15 дней | ⏳ идеи 473–474 |
| WASM-плагины | P3 | 10 дней | ⏳ идея 477 |
```

---

## Следующие шаги реализации (после этого документа)

Критичный путь к работающему MVP (Milestone 0–1 из roadmap):

1. ✅ **Готово в документации**: ядро, InMemory, RabbitMQ, Outbox, Sagas, Generator, тесты, CI
2. ⏭ **Собрать реальный solution** по структуре из файла 12
3. ⏭ **Прогнать тесты** из файла 13 — довести до зелёного
4. ⏭ **Kafka transport** — довести скетч до полной реализации
5. ⏭ **Scheduling** — cron + отложенные (durable)
6. ⏭ **Event Sourcing** — EventStore + проекции
7. ⏭ **Первый публичный preview** 0.1.0 → NuGet
