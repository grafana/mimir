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

Tune your Grafana Mimir configuration to suit your specific environment. 

{{< admonition type="note" >}}
You can use Grafana Cloud to avoid installing, maintaining, and scaling your own instance of Grafana Mimir. [Create a free account to get started](https://grafana.com/auth/sign-up/create-user?pg=docs-mimir-latest-configure), which includes free forever access to 10k metrics, 50GB logs, 50GB traces, 500VUh k6 testing & more.{{< /admonition >}}

## Get started with your configuration

Review the fundamentals of configuring Grafana Mimir.

- [About Grafana Mimir configurations](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-configurations/)- Learn the basics of configuring Grafana Mimir.
- [Configuration parameters](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configuration-parameters/)- Review a reference of all configuration parameters.
- [Runtime configuration](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-runtime-configuration/)- Make dynamic configuration changes without restarting Grafana Mimir.
- [Versioning](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-versioning/)- Learn about Grafana Labs' guarantees for this major release of Grafana Mimir.

## Multi-tenancy and security

Set up secure, multi-tenant environments.

- [Tenant IDs](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-tenant-ids/)- Understand tenant identification and naming conventions.
- [High-availability deduplication](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-high-availability-deduplication/)- Handle HA Prometheus server pairs and deduplication.
- [Anonymous usage statistics](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-anonymous-usage-statistics-reporting/)- Manage telemetry and usage reporting.

## Storage and data management

Configure how Mimir stores and manages your metrics data.

- [Object storage backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-object-storage-backend/)- Set up S3, GCS, Azure Blob Storage, or Swift storage.
- [Metrics storage retention](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-metrics-storage-retention/)- Manage data lifecycle and automatic cleanup.
- [TSDB block upload](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-tsdb-block-upload/)- Import historical data from Prometheus or other sources.
- [Out-of-order samples ingestion](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-out-of-order-samples-ingestion/)- Handle late-arriving or backfilled data.

## Ingestion and data sources

Configure how data is ingested into Mimir.

- [Native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-native-histograms-ingestion/)- Configure Prometheus native histogram ingestion.
- [OpenTelemetry Collector](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-otel-collector/)- Set up OTLP or Prometheus remote write from OTel.
- [Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/)- (Experimental) Use Kafka for ingest storage.

## Performance and scaling

Manage sharding, distribution, resource management, and limits.

- **[Shuffle sharding](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-shuffle-sharding/)** - Isolate tenants and reduce blast radius of outages
- **[Spread-minimizing tokens](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-spread-minimizing-tokens/)** - Migrate ingesters for optimal token distribution
- **[Hash rings](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-hash-rings/)** - Configure consistent hashing and key-value stores
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
