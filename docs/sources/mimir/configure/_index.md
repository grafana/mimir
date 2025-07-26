---
aliases:
  - operators-guide/configure/
  - operators-guide/configuring/
description: This section provides links to Grafana Mimir configuration topics.
keywords:
  - Mimir configuration
menuTitle: Configure
title: Configure Grafana Mimir
weight: 30
---

# Configure Grafana Mimir

Tune the Grafana Mimir configuration to suit your environment. This guide organizes configuration topics by common use cases and user journey to help you find the right information quickly.

## Getting Started with Configuration

Before diving into specific configurations, understand the fundamentals:

- **[About Grafana Mimir configurations](about-configurations.md)** - Learn configuration basics, best practices, and operational considerations
- **[Configuration parameters](configuration-parameters/)** - Complete reference of all available configuration parameters
- **[Runtime configuration](about-runtime-configuration.md)** - Dynamic configuration changes without restarts

## Multi-tenancy and Security

Set up secure, multi-tenant environments:

- **[Tenant IDs](about-tenant-ids.md)** - Understand tenant identification and naming restrictions
- **[High-availability deduplication](configure-high-availability-deduplication.md)** - Handle HA Prometheus server pairs and deduplication
- **[Anonymous usage statistics](about-anonymous-usage-statistics-reporting.md)** - Control telemetry and usage reporting

## Storage and Data Management

Configure how Mimir stores and manages your metrics data:

- **[Object storage backend](configure-object-storage-backend.md)** - Set up S3, GCS, Azure Blob Storage, or Swift storage
- **[Metrics storage retention](configure-metrics-storage-retention.md)** - Control data lifecycle and automatic cleanup
- **[TSDB block upload](configure-tsdb-block-upload.md)** - Import historical data from Prometheus or other sources
- **[Out-of-order samples ingestion](configure-out-of-order-samples-ingestion.md)** - Handle late-arriving or backfilled data

## Ingestion and Data Sources

Configure how data flows into Mimir:

- **[Native histograms](configure-native-histograms-ingestion.md)** - Enable Prometheus native histogram support
- **[OpenTelemetry Collector](configure-otel-collector.md)** - Set up OTLP or Prometheus remote write from OTel
- **[Kafka backend](configure-kafka-backend.md)** - Use Kafka for ingest storage (experimental)

## Performance and Scaling

### Sharding and Distribution

Configure how Mimir distributes workload across instances:

- **[Shuffle sharding](configure-shuffle-sharding/)** - Isolate tenants and reduce blast radius of outages
- **[Spread-minimizing tokens](configure-spread-minimizing-tokens/)** - Migrate ingesters for optimal token distribution
- **[Hash rings](configure-hash-rings.md)** - Configure consistent hashing and key-value stores

### Resource Management and Limiting

Control resource utilization and protect against overload:

- **[Reactive limiters](about-reactive-limiters.md)** - Automatic concurrency limiting based on overload detection
- **[Ingester circuit breakers](about-ingester-circuit-breakers.md)** - Protect ingesters from slow requests
- **[Resource utilization limiting](configure-resource-utilization-based-ingester-read-path-limiting.md)** - CPU and memory-based read path limiting
- **[Custom active series trackers](configure-custom-trackers.md)** - Monitor active series by custom label patterns

## Query Management

Control and optimize query behavior:

- **[Block queries](configure-blocked-queries.md)** - Prevent expensive or unwanted queries
- **[Experimental PromQL functions](configure-experimental-promql-functions.md)** - Enable new PromQL features selectively
- **[Query-frontend with Prometheus](configure-the-query-frontend-work-with-prometheus.md)** - Use Mimir's query-frontend with existing Prometheus

## High Availability and Clustering

Configure Mimir for production resilience:

- **[Zone-aware replication](configure-zone-aware-replication.md)** - Replicate data across failure domains
- **[Mirror requests to a second cluster](mirror-requests-to-a-second-cluster/)** - Set up request mirroring for testing
- **[DNS service discovery](about-dns-service-discovery.md)** - Configure service discovery for clustering

## Observability and Troubleshooting

Monitor and debug your Mimir deployment:

- **[Tracing](configure-tracing.md)** - Set up distributed tracing with OpenTelemetry or Jaeger
- **[IP address logging](about-ip-address-logging.md)** - Log client IPs when behind reverse proxies

## Advanced Topics

- **[Versioning](about-versioning.md)** - Understand Mimir's compatibility guarantees and feature lifecycle

---

## All Configuration Topics

For a complete alphabetical list of all configuration topics, see the topics listed in the sidebar navigation.
