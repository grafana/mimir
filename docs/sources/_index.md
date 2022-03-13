---
title: "Grafana Mimir technical documentation"
weight: 1
---

# Grafana Mimir Documentation

![Grafana Mimir](./images/mimir-logo.png)

Grafana Mimir is an open source software project that provides horizontally scalable long-term storage for [Prometheus](https://prometheus.io). Its core feature set includes: 

- **Massively scalable:** Mimir's horizontally scalable architecture makes it possible for it to exceed the resource constraints of a single machine and handle orders of magnitude more time-serise than a single Prometheus. Mimir has been tested at up to 1 billion active time-series. 
- **Global view of metrics:** Run queries that aggregate series across multiple Prometheus instances to get a global view of your systems. Mimir's highly optimized query engine breaks incoming queries into smaller, parallelizable operations, so that even this highest cardinality queries excute with blazing speed.  
- **Highly availability:** Grafana Mimir replicates incoming metrics data among machines, ensuring that no data is lost in the event of machine failure. Its horziointally scalable architecture also means that it can be restarted, upgraded, or downgraded with zero downtime. 
- **Multi-tenancy:** Grafana Mimir can isolate data and queries from multiple independent
  Prometheus sources in a single cluster, allowing untrusted parties to share the same cluster.
- **Cheap, durable metric storage:** Grafana Mimir uses object storage for long-term storage of metric data. It is compatible with a variety of object stores, including AWS S3, Google Cloud Storage, Azure Blob Storage, Swift, and any S3-compliant object store (e.g. MinIO). 

> **Note:** You can use [Grafana Cloud](https://grafana.com/products/cloud/features/#cloud-metrics) to avoid installing, maintaining, and scaling your own instance of Grafana Mimir. The free forever plan includes 10,000 metrics. [Create an account to get started](https://grafana.com/auth/sign-up/create-user?pg=docs-mimir&plcmt=in-text).
