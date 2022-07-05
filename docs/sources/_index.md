---
title: "Grafana Mimir documentation"
menuTitle: "Grafana Mimir"
weight: 1
keywords:
  - Grafana Mimir
  - Grafana metrics
  - time series database
  - TSDB
  - Prometheus storage
  - Prometheus remote write
  - metrics storage
  - metrics datastore
  - observability
---

# Grafana Mimir documentation

![Grafana Mimir](mimir-logo.png)

Grafana Mimir is an open source software project that provides a scalable long-term storage for [Prometheus](https://prometheus.io). Some of the core strengths of Grafana Mimir include:

- **Easy to install and maintain:** Grafana Mimir’s extensive documentation, tutorials, and deployment tooling make it quick to get started. Using its monolithic mode, you can get Grafana Mimir up and running with just one binary and no additional dependencies. Once deployed, the best-practice dashboards, alerts, and runbooks packaged with Grafana Mimir make it easy to monitor the health of the system.
- **Massive scalability:** You can run Grafana Mimir's horizontally-scalable architecture across multiple machines, resulting in the ability to process orders of magnitude more time series than a single Prometheus instance. Internal testing shows that Grafana Mimir handles up to 1 billion active time series.
- **Global view of metrics:** Grafana Mimir enables you to run queries that aggregate series from multiple Prometheus instances, giving you a global view of your systems. Its query engine extensively parallelizes query execution, so that even the highest-cardinality queries complete with blazing speed.
- **Cheap, durable metric storage:** Grafana Mimir uses object storage for long-term data storage, allowing it to take advantage of this ubiquitous, cost-effective, high-durability technology. It is compatible with multiple object store implementations, including AWS S3, Google Cloud Storage, Azure Blob Storage, OpenStack Swift, as well as any S3-compatible object storage.
- **High availability:** Grafana Mimir replicates incoming metrics, ensuring that no data is lost in the event of machine failure. Its horizontally scalable architecture also means that it can be restarted, upgraded, or downgraded with zero downtime, which means no interruptions to metrics ingestion or querying.
- **Natively multi-tenant:** Grafana Mimir’s multi-tenant architecture enables you to isolate data and queries from independent teams or business units, making it possible for these groups to share the same cluster. Advanced limits and quality-of-service controls ensure that capacity is shared fairly among tenants.

> **Note:** You can use [Grafana Cloud](https://grafana.com/products/cloud/features/#cloud-metrics) to avoid installing, maintaining, and scaling your own instance of Grafana Mimir. The free forever plan includes 10,000 metrics. [Create an account to get started](https://grafana.com/auth/sign-up/create-user?pg=docs-mimir&plcmt=in-text).
