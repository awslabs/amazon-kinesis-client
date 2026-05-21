# KCL OpenTelemetry Metrics - Grafana Dashboard

Pre-built Grafana dashboard for visualizing Amazon Kinesis Client Library (KCL) metrics emitted via the OTel native instrumentation backend.

## Dashboard Sections

| Section | Panels | Key Metrics |
|---------|--------|-------------|
| **Overview** | 4 stat panels | MillisBehindLatest, Records/s, Current Leases, Lost Leases |
| **Record Processing** | 4 time series | MillisBehindLatest by shard, Records processed rate, Data bytes rate, processRecords latency p99 |
| **Lease Management** | 3 time series | Lease counts, Lease activity (taken/lost/expired), Workers & balance |
| **Operation Latency & Success** | 2 time series | Operation time p99 by operation, Success rate by operation |
| **Iterator & Fetcher** | 2 time series | Expired iterators, KinesisDataFetcher latency p99 |

## Prerequisites

1. **KCL configured with `MetricsBackend.OTEL`** — the native OTel instrumentation backend
2. **OTel SDK** on the classpath with a `MetricExporter` configured (e.g., OTLP exporter)
3. **Prometheus-compatible backend** receiving the OTel metrics (e.g., Amazon Managed Prometheus, Prometheus with OTLP receiver, Grafana Mimir)
4. **Grafana** with a Prometheus data source configured

## Setup

### 1. Import the Dashboard

1. Open Grafana → Dashboards → Import
2. Upload `kcl-otel-dashboard.json` or paste its contents
3. Select your Prometheus data source when prompted

### 2. Template Variables

The dashboard includes three template variables for filtering:

- **`$consumer`** — Filter by KCL application name (`aws_kinesis_consumer_name` label)
- **`$stream`** — Filter by Kinesis stream (`aws_kinesis_stream_name` label)
- **`$shard`** — Filter by shard ID (`aws_kinesis_shard_id` label, multi-select)

### 3. OTel Collector Configuration (if using OTel Collector)

Example collector config to receive KCL metrics and export to Prometheus:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

exporters:
  prometheusremotewrite:
    endpoint: "https://aps-workspaces.<region>.amazonaws.com/workspaces/<workspace-id>/api/v1/remote_write"
    auth:
      authenticator: sigv4auth

extensions:
  sigv4auth:
    region: us-east-1

service:
  extensions: [sigv4auth]
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [prometheusremotewrite]
```

### 4. KCL Application Configuration

```java
// Configure KCL with OTel backend
MetricsConfig metricsConfig = configsBuilder.metricsConfig()
    .metricsBackend(MetricsBackend.OTEL)
    .metricsLevel(MetricsLevel.DETAILED)
    .metricsEnabledDimensions(Set.of("Operation", "ShardId", "StreamId", "WorkerIdentifier"));
```

## Metric Name Mapping

KCL metrics are recorded via OTel instruments. The Prometheus metric names follow OTel-to-Prometheus conventions:

| KCL Metric | OTel Instrument | Prometheus Name |
|---|---|---|
| RecordsProcessed | LongCounter | `RecordsProcessed_total` or `RecordsProcessed_count` |
| DataBytesProcessed | LongCounter | `DataBytesProcessed_total` |
| MillisBehindLatest | DoubleHistogram | `MillisBehindLatest` / `MillisBehindLatest_bucket` |
| Time | DoubleHistogram | `Time_bucket` |
| Success | LongCounter | `Success_total` |
| RecordProcessor.processRecords | DoubleHistogram | `RecordProcessor_processRecords_bucket` |

## OTel Attribute Labels

| KCL Dimension | Prometheus Label |
|---|---|
| Operation | `aws_kinesis_operation` |
| ShardId | `aws_kinesis_shard_id` |
| StreamId | `aws_kinesis_stream_name` |
| WorkerIdentifier | `aws_kinesis_consumer_name` |

## Customization

- Adjust `[5m]` rate windows to match your scrape interval
- Add alerting rules for `MillisBehindLatest > threshold` or `LostLeases > 0`
- Add additional panels for migration metrics if using KCL 2.x → 3.x migration
