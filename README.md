# Grafana Mimir

<p align="center"><img src="images/logo.png" alt="Grafana Mimir logo" width="400"></p>

Grafana Mimir is an open source software project that provides a scalable long-term storage for [Prometheus](https://prometheus.io). Some of the core strengths of Grafana Mimir include:

- **Easy to install and maintain:** Grafana Mimir’s extensive documentation, tutorials, and deployment tooling make it quick to get started. Using its monolithic mode, you can get Grafana Mimir up and running with just one binary and no additional dependencies. Once deployed, the best-practice dashboards, alerts, and playbooks packaged with Grafana Mimir make it easy to monitor the health of the system.
- **Massive scalability:** You can run Grafana Mimir's horizontally-scalable architecture across multiple machines, resulting in the ability to process orders of magnitude more time series than a single Prometheus instance. Internal testing shows that Grafana Mimir handles up to 1 billion active time series.
- **Global view of metrics:** Grafana Mimir enables you to run queries that aggregate series from multiple Prometheus instances, giving you a global view of your systems. Its query engine extensively parallelizes query execution, so that even the highest-cardinality queries complete with blazing speed.
- **Cheap, durable metric storage:** Grafana Mimir uses object storage for long-term data storage, allowing it to take advantage of this ubiquitous, cost-effective, high-durability technology. It is compatible with multiple object store implementations, including AWS S3, Google Cloud Storage, Azure Blob Storage, OpenStack Swift, as well as any S3-compatible object storage.
- **High availability:** Grafana Mimir replicates incoming metrics, ensuring that no data is lost in the event of machine failure. Its horizontally scalable architecture also means that it can be restarted, upgraded, or downgraded with zero downtime, which means no interruptions to metrics ingestion or querying.
- **Natively multi-tenant:** Grafana Mimir’s multi-tenant architecture enables you to isolate data and queries from independent teams or business units, making it possible for these groups to share the same cluster. Advanced limits and quality-of-service controls ensure that capacity is shared fairly among tenants.

## Migrating to Grafana Mimir

If you're migrating to Grafana Mimir, refer to the following documents:

- [Migrating from Thanos or Prometheus to Grafana Mimir](https://grafana.com/docs/mimir/latest/migration-guide/migrating-from-thanos-or-prometheus/).
- [Migrating from Cortex to Grafana Mimir](https://grafana.com/docs/mimir/latest/migration-guide/migrating-from-cortex/)

## Deploying Grafana Mimir

For information about how to deploy Grafana Mimir, refer to [Deploying Grafana Mimir](https://grafana.com/docs/mimir/latest/operators-guide/deploying-grafana-mimir/).

## Getting started

If you’re new to Grafana Mimir, read the [Getting started guide](https://grafana.com/docs/mimir/latest/operators-guide/getting-started/).

Before deploying Grafana Mimir in a production environment, read:

1. [An overview of Grafana Mimir’s architecture](https://grafana.com/docs/mimir/latest/operators-guide/architecture/)
1. [Configuring Grafana Mimir](https://grafana.com/docs/mimir/latest/operators-guide/configuring/)
1. [Running Grafana Mimir in production](https://grafana.com/docs/mimir/latest/operators-guide/running-production-environment/)

## Documentation

Refer to the following links to access Grafana Mimir documentation:

- [Latest release](https://grafana.com/docs/mimir/latest/)
- [Upcoming release](https://grafana.com/docs/mimir/next/), at the tip of the main branch

## Contributing

To contribute to Grafana Mimir, refer to [Contributing to Grafana Mimir](https://github.com/grafana/mimir/tree/main/docs/internal/contributing).

## Join the Grafana Mimir discussion

If you have any questions or feedback regarding Grafana Mimir, join the [Grafana Mimir Discussion](https://github.com/grafana/mimir/discussions). Alternatively, consider joining the monthly [Grafana Mimir Community Call](https://docs.google.com/document/d/1E4jJcGicvLTyMEY6cUFFZUg_I8ytrBuW8r5yt1LyMv4).

Your feedback is always welcome, and you can also share it via the [`#mimir` Slack channel](https://slack.grafana.com/).

## License

Grafana Mimir is distributed under [AGPL-3.0-only](LICENSE).
