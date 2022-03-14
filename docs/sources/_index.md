---
title: "Grafana Mimir technical documentation"
weight: 1
---

# Grafana Mimir Documentation

![Grafana Mimir](./images/mimir-logo.png)

Grafana Mimir is an open source software project that provides horizontally scalable long-term storage for [Prometheus](https://prometheus.io). Some of the core strengths of Grafana Mimir include: 

- **Massive scalability:** Grafana Mimir's horizontally scalable architecture enables it to be run across multiple machines and handle orders of magnitude more time-series than a single Prometheus. Grafana Mimir has been proven to handle up to 1 billion active time-series. 
- **Global view of metrics:** Grafana Mimir allows you to run queries that aggregate series from multiple Prometheus instances, giving you a global view of your systems. Its query engine extensively parallelizes query execution so that even the highest cardinality queries complete with blazing speed.  
- **Cheap, durable metric storage:** Grafana Mimir uses object storage for long-term data storage, allowing it to take advantage of the ubiquity, cost-effectiveness, and high durability of this technology. It is compatible with a variety of object stores, including AWS S3, Google Cloud Storage, Azure Blob Storage, Swift, as well as any S3-compatible object storage. 
- **High availability:** Grafana Mimir replicates incoming metrics, ensuring that no data is lost in the event of machine failure. Its horizontally scalable architecture also means that it can be restarted, upgraded, or downgraded with zero downtime, which means no interruptions to metrics ingestion or querying. 
- **Natively multi-tenant:** Grafana Mimir’s multi-tenant architecture allows you to isolate data and queries from independent teams or business units, making it possible for these groups to share the same cluster. Advanced limits and quality-of-service controls ensure that capacity is shared fairly among tenants. 


> **Note:** You can use [Grafana Cloud](https://grafana.com/products/cloud/features/#cloud-metrics) to avoid installing, maintaining, and scaling your own instance of Grafana Mimir. The free forever plan includes 10,000 metrics. [Create an account to get started](https://grafana.com/auth/sign-up/create-user?pg=docs-mimir&plcmt=in-text).
