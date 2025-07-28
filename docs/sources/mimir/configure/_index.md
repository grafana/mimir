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

## Get started with your configuration

Before diving into specific configurations, understand the fundamentals:

- **[About Grafana Mimir configurations](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-configurations/)** - Learn configuration basics, best practices, and operational considerations
- **[Configuration parameters](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configuration-parameters/)** - Complete reference of all available configuration parameters
- **[Runtime configuration](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-runtime-configuration/)** - Dynamic configuration changes without restarts
- **[Versioning](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-versioning/)** - Understand Mimir's compatibility guarantees and feature lifecycle

## Multi-tenancy and security

Set up secure, multi-tenant environments:

- **[Tenant IDs](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-tenant-ids/)** - Understand tenant identification and naming restrictions
- **[High-availability deduplication](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-high-availability-deduplication/)** - Handle HA Prometheus server pairs and deduplication
- **[Anonymous usage statistics](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-anonymous-usage-statistics-reporting/)** - Control telemetry and usage reporting

## Storage and data management

Configure how Mimir stores and manages your metrics data:

- **[Object storage backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-object-storage-backend/)** - Set up S3, GCS, Azure Blob Storage, or Swift storage
- **[Metrics storage retention](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-metrics-storage-retention/)** - Control data lifecycle and automatic cleanup
- **[TSDB block upload](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-tsdb-block-upload/)** - Import historical data from Prometheus or other sources
- **[Out-of-order samples ingestion](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-out-of-order-samples-ingestion/)** - Handle late-arriving or backfilled data

## Ingestion and data sources

Configure how data flows into Mimir:

- **[Native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-native-histograms-ingestion/)** - Enable Prometheus native histogram support
- **[OpenTelemetry Collector](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-otel-collector/)** - Set up OTLP or Prometheus remote write from OTel
- **[Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/)** - Use Kafka for ingest storage (experimental)

## Performance and scaling

### Sharding and distribution

Configure how Mimir distributes workload across instances:

- **[Shuffle sharding](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-shuffle-sharding/)** - Isolate tenants and reduce blast radius of outages
- **[Spread-minimizing tokens](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-spread-minimizing-tokens/)** - Migrate ingesters for optimal token distribution
- **[Hash rings](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-hash-rings/)** - Configure consistent hashing and key-value stores

### Resource management and limiting

Control resource utilization and protect against overload:

- **[Reactive limiters](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-reactive-limiters/)** - Automatic concurrency limiting based on overload detection
- **[Ingester circuit breakers](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-ingester-circuit-breakers/)** - Protect ingesters from slow requests
- **[Resource utilization limiting](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-resource-utilization-based-ingester-read-path-limiting/)** - CPU and memory-based read path limiting
- **[Custom active series trackers](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-custom-trackers/)** - Monitor active series by custom label patterns

## Query management

Control and optimize query behavior:

- **[Block queries](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-blocked-queries/)** - Prevent expensive or unwanted queries
- **[Experimental PromQL functions](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-experimental-promql-functions/)** - Enable new PromQL features selectively
- **[Query-frontend with Prometheus](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-the-query-frontend-work-with-prometheus/)** - Use Mimir's query-frontend with existing Prometheus

## High availability and clustering

Configure Mimir for production resilience:

- **[Zone-aware replication](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-zone-aware-replication/)** - Replicate data across failure domains
- **[Mirror requests to a second cluster](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/mirror-requests-to-a-second-cluster/)** - Set up request mirroring for testing
- **[DNS service discovery](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-dns-service-discovery/)** - Configure service discovery for clustering

## Observability and troubleshooting

Monitor and debug your Mimir deployment:

- **[Tracing](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-tracing/)** - Set up distributed tracing with OpenTelemetry or Jaeger
- **[IP address logging](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-ip-address-logging/)** - Log client IPs when behind reverse proxies
