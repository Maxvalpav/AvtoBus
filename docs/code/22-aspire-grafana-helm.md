# AvtoBus — Aspire, Grafana, Helm, Event Catalog

Инфраструктурные интеграции и DevOps.

> **Реализовано**: `src/AvtoBus.Aspire` (`AspireExtensions.cs`) — `AddAvtoBusRabbit`,
> `WithAvtoBus`, `WithAvtoBusPostgres`; 3 теста построения модели ресурсов (`tests/AvtoBus.AspireTests`).
> Aspire 13.4.x.

---

## AvtoBus.Aspire/AspireExtensions.cs

```csharp
using Aspire.Hosting;
using Aspire.Hosting.ApplicationModel;

namespace AvtoBus.Aspire;

/// <summary>
/// .NET Aspire integration: добавляем AvtoBus ресурсы в AppHost.
/// </summary>
public static class AspireExtensions
{
    /// <summary>
    /// Добавить RabbitMQ + AvtoBus Dashboard как ресурсы Aspire.
    /// </summary>
    public static IResourceBuilder<RabbitMQServerResource> AddAvtoBusRabbit(
        this IDistributedApplicationBuilder builder,
        string name = "avtobus-rabbit")
    {
        return builder.AddRabbitMQ(name)
            .WithManagementPlugin()
            .WithLifetime(ContainerLifetime.Persistent);
    }

    /// <summary>
    /// Подключить проект к AvtoBus RabbitMQ.
    /// </summary>
    public static IResourceBuilder<ProjectResource> WithAvtoBus(
        this IResourceBuilder<ProjectResource> project,
        IResourceBuilder<RabbitMQServerResource> rabbit,
        IResourceBuilder<PostgresServerResource>? postgres = null)
    {
        var result = project.WithReference(rabbit);

        if (postgres is not null)
            result = result.WithReference(postgres);

        return result.WithEnvironment("AVTOBUS_TRANSPORT", "rabbitmq");
    }
}
```

Пример Aspire AppHost:

```csharp
// AppHost/Program.cs
var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("pg").AddDatabase("orders");
var rabbit = builder.AddAvtoBusRabbit();
var jaeger = builder.AddContainer("jaeger", "jaegertracing/all-in-one").WithEndpoint(4317, 4317).WithEndpoint(16686, 16686, "ui");

builder.AddProject<Projects.OrderService>("orders")
    .WithAvtoBus(rabbit, postgres)
    .WithReference(jaeger);

builder.AddProject<Projects.PaymentService>("payments")
    .WithAvtoBus(rabbit);

builder.Build().Run();
```

---

## monitoring/grafana/avtobus-dashboard.json

```json
{
  "dashboard": {
    "title": "AvtoBus Overview",
    "uid": "avtobus-overview",
    "panels": [
      {
        "title": "Throughput (msg/s)",
        "type": "timeseries",
        "targets": [{
          "expr": "rate(avtobus_consume_count_total[1m])",
          "legendFormat": "{{type}}"
        }],
        "gridPos": { "x": 0, "y": 0, "w": 12, "h": 8 }
      },
      {
        "title": "Consume p95 latency (ms)",
        "type": "timeseries",
        "targets": [{
          "expr": "histogram_quantile(0.95, rate(avtobus_consume_duration_bucket[5m]))",
          "legendFormat": "p95"
        }],
        "gridPos": { "x": 12, "y": 0, "w": 12, "h": 8 }
      },
      {
        "title": "Critical Time p99 (ms)",
        "type": "stat",
        "targets": [{
          "expr": "histogram_quantile(0.99, rate(avtobus_critical_time_bucket[5m]))"
        }],
        "gridPos": { "x": 0, "y": 8, "w": 6, "h": 4 }
      },
      {
        "title": "DLQ Messages",
        "type": "stat",
        "targets": [{ "expr": "avtobus_dead_lettered_total" }],
        "gridPos": { "x": 6, "y": 8, "w": 6, "h": 4 },
        "thresholds": { "steps": [
          { "value": 0, "color": "green" },
          { "value": 1, "color": "orange" },
          { "value": 10, "color": "red" }
        ]}
      },
      {
        "title": "Outbox Pending",
        "type": "gauge",
        "targets": [{ "expr": "avtobus_outbox_pending" }],
        "gridPos": { "x": 12, "y": 8, "w": 6, "h": 4 },
        "thresholds": { "steps": [
          { "value": 0, "color": "green" },
          { "value": 1000, "color": "yellow" },
          { "value": 10000, "color": "red" }
        ]}
      },
      {
        "title": "Active Sagas",
        "type": "stat",
        "targets": [{ "expr": "avtobus_saga_started_total - avtobus_saga_completed_total - avtobus_saga_aborted_total" }],
        "gridPos": { "x": 18, "y": 8, "w": 6, "h": 4 }
      },
      {
        "title": "Retries/s",
        "type": "timeseries",
        "targets": [{ "expr": "rate(avtobus_retry_total[1m])", "legendFormat": "retries" }],
        "gridPos": { "x": 0, "y": 12, "w": 12, "h": 6 }
      },
      {
        "title": "Inbox Deduplication",
        "type": "timeseries",
        "targets": [{ "expr": "rate(avtobus_inbox_deduped_total[1m])", "legendFormat": "deduped" }],
        "gridPos": { "x": 12, "y": 12, "w": 12, "h": 6 }
      },
      {
        "title": "Publish Duration p95 (ms)",
        "type": "timeseries",
        "targets": [{
          "expr": "histogram_quantile(0.95, rate(avtobus_publish_duration_bucket[5m]))",
          "legendFormat": "p95"
        }],
        "gridPos": { "x": 0, "y": 18, "w": 12, "h": 6 }
      },
      {
        "title": "Consumer Errors/s",
        "type": "timeseries",
        "targets": [{
          "expr": "rate(avtobus_consume_errors_total[1m])",
          "legendFormat": "{{type}}"
        }],
        "gridPos": { "x": 12, "y": 18, "w": 12, "h": 6 }
      }
    ]
  }
}
```

---

## deploy/helm/avtobus-worker/Chart.yaml

```yaml
apiVersion: v2
name: avtobus-worker
description: Helm chart for AvtoBus worker service
version: 0.1.0
appVersion: "0.1.0"
```

## deploy/helm/avtobus-worker/values.yaml

```yaml
replicaCount: 2

image:
  repository: ghcr.io/avtobus/sample-worker
  tag: latest
  pullPolicy: IfNotPresent

env:
  AVTOBUS_TRANSPORT: rabbitmq
  ConnectionStrings__Rabbit: amqp://guest:guest@rabbit:5672
  ConnectionStrings__Db: Host=postgres;Database=app;Username=app;Password=app
  OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector:4317

resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: "1"
    memory: 512Mi

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: External
      external:
        metric:
          name: avtobus_queue_depth
          selector:
            matchLabels:
              queue: orders
        target:
          type: AverageValue
          averageValue: "100"

livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 5

readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 10

startupProbe:
  httpGet:
    path: /startupz
    port: 8080
  failureThreshold: 30
  periodSeconds: 2

terminationGracePeriodSeconds: 45

podAnnotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8080"
  prometheus.io/path: "/metrics"
```

## deploy/helm/avtobus-worker/templates/deployment.yaml

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Chart.Name }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      app: {{ .Chart.Name }}
  template:
    metadata:
      labels:
        app: {{ .Chart.Name }}
      annotations:
        {{- toYaml .Values.podAnnotations | nindent 8 }}
    spec:
      terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
      containers:
        - name: worker
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          ports:
            - containerPort: 8080
          env:
            {{- range $key, $value := .Values.env }}
            - name: {{ $key }}
              value: {{ $value | quote }}
            {{- end }}
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
          livenessProbe:
            {{- toYaml .Values.livenessProbe | nindent 12 }}
          readinessProbe:
            {{- toYaml .Values.readinessProbe | nindent 12 }}
          startupProbe:
            {{- toYaml .Values.startupProbe | nindent 12 }}
```

---

## deploy/keda/scaledobject.yaml (автоскейлинг по глубине очереди)

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: avtobus-orders-scaler
spec:
  scaleTargetRef:
    name: orders-worker
  minReplicaCount: 1
  maxReplicaCount: 30
  cooldownPeriod: 60
  triggers:
    - type: rabbitmq
      metadata:
        host: amqp://guest:guest@rabbit.default.svc:5672/
        queueName: orders
        mode: QueueLength
        value: "50"
    - type: rabbitmq
      metadata:
        host: amqp://guest:guest@rabbit.default.svc:5672/
        queueName: orders.retry.5s
        mode: QueueLength
        value: "10"
```

---

## docs/event-catalog/catalog.yaml (Event Catalog)

```yaml
# Автогенерируется из compile-time модели командой `avtobus catalog generate`
catalog:
  name: AvtoBus E-Commerce
  version: 1.0.0
  description: Каталог всех событий, команд и подписок

domains:
  - name: Orders
    owner: team-orders
    events:
      - name: OrderPlaced
        version: "1"
        description: Заказ размещён клиентом
        schema: { $ref: '#/schemas/OrderPlaced' }
        publishers: [OrderService]
        subscribers: [PaymentService, ShippingService, AnalyticsService]
      - name: OrderPaid
        version: "1"
        description: Оплата заказа подтверждена
        publishers: [OrderService]
        subscribers: [ShippingService, NotificationService]

  - name: Payments
    owner: team-payments
    commands:
      - name: ChargeCard
        version: "1"
        handler: PaymentService
        sent_by: [OrderService]
    events:
      - name: PaymentSucceeded
        version: "1"
        publishers: [PaymentService]
        subscribers: [OrderService]

schemas:
  OrderPlaced:
    type: object
    properties:
      orderId: { type: string, format: uuid }
      customerId: { type: string }
      total: { type: number }
    required: [orderId, customerId, total]
```

CLI для генерации:

```bash
# Генерировать каталог из compile-time модели
avtobus catalog generate --output docs/event-catalog/

# Поднять интерактивный UI
avtobus catalog serve --port 3000

# Экспортировать статический сайт
avtobus catalog build --output dist/catalog/
```

---

## AvtoBus.Aspire.csproj

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Description>AvtoBus integration for .NET Aspire AppHost.</Description>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Aspire.Hosting.RabbitMQ" />
    <PackageReference Include="Aspire.Hosting.PostgreSQL" />
  </ItemGroup>
</Project>
```
