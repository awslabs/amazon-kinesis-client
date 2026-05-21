# KCL OTel Sample App

End-to-end sample demonstrating the Amazon Kinesis Client Library (KCL) with OpenTelemetry metrics flowing to CloudWatch, deployed on ECS Fargate via CDK, with an Amazon Managed Grafana dashboard for visualization.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        AWS Account                               │
│                                                                  │
│  ┌──────────┐    ┌─────────────────────────────────────────┐    │
│  │  Kinesis  │◄───│  Data Producer (local or ECS task)       │    │
│  │  Stream   │    └─────────────────────────────────────────┘    │
│  └────┬─────┘                                                    │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  ECS Fargate Service                                     │    │
│  │  ┌───────────────────────────────────────────────────┐  │    │
│  │  │  KCL Scheduler (MetricsBackend.OTEL)              │  │    │
│  │  │  ┌─────────────┐  ┌──────────────────────────┐   │  │    │
│  │  │  │ OTel SDK     │──│ OTLP HTTP Exporter        │   │  │    │
│  │  │  └─────────────┘  └────────────┬─────────────┘   │  │    │
│  │  └─────────────────────────────────┼─────────────────┘  │    │
│  └────────────────────────────────────┼─────────────────────┘    │
│                                       │                          │
│                                       ▼                          │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  CloudWatch (OTLP Endpoint)                              │    │
│  │  Metrics: RecordsProcessed, processRecords.Time, etc.    │    │
│  └────────────────────────────────────┬─────────────────────┘    │
│                                       │                          │
│                                       ▼                          │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Amazon Managed Grafana                                   │    │
│  │  Dashboard: KCL OTel Metrics (PromQL queries)            │    │
│  └─────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- AWS CLI configured with appropriate credentials
- AWS CDK CLI (`npm install -g aws-cdk`)
- Docker (for building the container image)
- Maven 3.8+ and Java 17+
- IAM Identity Center (SSO) configured in your account (for Managed Grafana)

## Quick Start

```bash
# 1. Deploy everything (Kinesis stream, ECS Fargate, Grafana workspace)
./scripts/deploy.sh

# 2. Produce test data to the stream
./scripts/produce-data.sh

# 3. Wait 2-3 minutes for metrics to flow through

# 4. Provision the Grafana dashboard
./scripts/provision-dashboard.sh

# 5. Open the Grafana URL from the deploy output
```

## Project Structure

```
sample-app/
├── README.md                    # This file
├── app/                         # KCL sample application (Java)
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/.../
│       ├── SampleApp.java              # Main entry point
│       ├── SampleAppConfig.java        # Env var configuration
│       ├── OTelSdkSetup.java           # OTel SDK initialization
│       ├── SampleRecordProcessor.java  # Record processing logic
│       ├── SampleRecordProcessorFactory.java
│       └── SampleDataProducer.java     # Test data generator
├── infrastructure/              # CDK stack (Java)
│   ├── pom.xml
│   ├── cdk.json
│   └── src/main/java/.../
│       ├── InfraApp.java               # CDK app entry
│       └── KclOtelSampleStack.java     # Full stack definition
├── grafana/                     # Grafana dashboard
│   ├── kcl-otel-dashboard.json         # Dashboard JSON model
│   └── README.md                       # Dashboard docs
└── scripts/
    ├── deploy.sh                # Deploy via CDK
    ├── produce-data.sh          # Run data producer
    ├── provision-dashboard.sh   # Import dashboard to Grafana
    └── destroy.sh               # Tear down all resources
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `STREAM_NAME` | `kcl-otel-sample-stream` | Kinesis stream name |
| `APPLICATION_NAME` | `kcl-otel-sample` | KCL app name (CloudWatch namespace) |
| `AWS_REGION` | `us-west-2` | AWS region |
| `OTLP_ENDPOINT` | `https://otlp.{region}.amazonaws.com/v1/metrics` | CloudWatch OTLP endpoint |
| `EXPORT_INTERVAL_MILLIS` | `60000` | OTel metric export interval |
| `PRODUCER_RECORDS_PER_SECOND` | `10` | Data producer rate |
| `PRODUCER_DURATION_SECONDS` | `300` | Data producer duration |

## How It Works

1. **OTel SDK Setup** — `OTelSdkSetup.initialize()` creates an `OtlpHttpMetricExporter` pointing to CloudWatch's OTLP endpoint, wraps it in a `PeriodicMetricReader`, and registers the SDK as `GlobalOpenTelemetry`.

2. **KCL Configuration** — The `Scheduler` is built with `MetricsBackend.OTEL` and `MetricsLevel.DETAILED`. The KCL's `OtelMetricsFactory` discovers the global OTel instance and records raw observations on `DoubleHistogram` and `LongCounter` instruments.

3. **Metrics Flow** — Every 60 seconds, the OTel SDK aggregates buffered observations and exports them via OTLP/HTTP to CloudWatch. Metrics appear under the configured namespace with OTel semantic convention attributes.

4. **Visualization** — Amazon Managed Grafana queries CloudWatch metrics using the PromQL-compatible endpoint, enabling `histogram_quantile()`, `rate()`, and label-based filtering.

## Cleanup

```bash
./scripts/destroy.sh
```

This removes all AWS resources created by the CDK stack.
