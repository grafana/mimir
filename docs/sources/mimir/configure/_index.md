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

- [About Grafana Mimir configurations](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-configurations/)
- [Grafana Mimir configuration parameters](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configuration-parameters/)
- [About Grafana Mimir runtime configuration](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-runtime-configuration/)
- [About Grafana Mimir versioning](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-versioning/)

## Multi-tenancy and security

Set up secure multi-tenant environments.

- [About Grafana Mimir tenant IDs](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-tenant-ids/)
- [Configure Grafana Mimir high-availability deduplication](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-high-availability-deduplication/)
- [About Grafana Mimir anonymous usage statistics reporting](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-anonymous-usage-statistics-reporting/)

## Storage and data management

Configure how Grafana Mimir stores and manages your data.

- [Configure Grafana Mimir object storage backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-object-storage-backend/)
- [Configure Grafana Mimir metrics storage retention](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-metrics-storage-retention/)
- [Configure TSDB block upload](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-tsdb-block-upload/)
- [Configure out-of-order samples ingestion](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-out-of-order-samples-ingestion/)

## Ingestion and data sources

Configure how data is ingested into Grafana Mimir.

- [Configure native histograms](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-native-histograms-ingestion/)
- [Configure the OpenTelemetry Collector to write metrics into Mimir](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-otel-collector/)
- [Configure the Grafana Mimir Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/)

## Performance and scaling

Manage sharding, distribution, resource management, and limits.

- [Configure Grafana Mimir shuffle sharding](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-shuffle-sharding/)
- [Migrate ingesters to spread-minimizing tokens](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-spread-minimizing-tokens/)
- [Configure Grafana Mimir hash rings](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-hash-rings/)
- [About Grafana Mimir reactive limiters](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-reactive-limiters/)
- [About Grafana Mimir ingester circuit breakers](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-ingester-circuit-breakers/)
- [Configure resource utilization based ingester read path limiting](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-resource-utilization-based-ingester-read-path-limiting/)
- [Configure custom active series trackers](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-custom-trackers/)

## Query management

Manage and optimize query behavior.

- [Configure queries to block](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-blocked-queries/)
- [Configure experimental PromQL functions](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-experimental-promql-functions/)
- [Configure the Grafana Mimir query-frontend to work with Prometheus](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-the-query-frontend-work-with-prometheus/)

## High availability and clustering

Configure Grafana Mimir for production resilience.

- [Configure Grafana Mimir zone-aware replication](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-zone-aware-replication/)
- [Mirror requests to a second Grafana Mimir cluster](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/mirror-requests-to-a-second-cluster/)
- [About Grafana Mimir DNS service discovery](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-dns-service-discovery/)

## Observability and troubleshooting

Monitor and debug your Grafana Mimir deployment.

- [Configure Grafana Mimir tracing](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-tracing/)
- [About Grafana Mimir IP address logging of a reverse proxy](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/about-ip-address-logging/)
