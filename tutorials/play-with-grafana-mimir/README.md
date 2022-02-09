# Play with Grafana Mimir

Grafana Mimir is a distributed, horizontally scalable and highly available long term storage for [Prometheus](https://prometheus.io).

In this tutorial, you'll:

- Run Grafana Mimir locally with Docker Compose
- Run Prometheus to scrape some metrics and remote write to Grafana Mimir
- Run Grafana to explore Grafana Mimir dashboards
- Configure a testing recording rule and alert in Grafana Mimir

## Prerequisites

- Git
- Docker and Docker Compose
- Latest `mimirtool` [release](https://github.com/grafana/mimir/releases/latest)
- Availability of both ports `9000` and `9009` on your host machine

## Download tutorial configuration

1. Create a copy of the Grafana Mimir repository using the Git command line:
   ```bash
   git clone https://github.com/grafana/mimir.git
   cd mimir
   ```
1. Move to the tutorial directory (the rest of the tutorial assumes you're always inside this directory)
   ```bash
   cd tutorials/play-with-grafana-mimir/
   ```

## Start Grafana Mimir and dependencies

Start running your local setup with the following Docker command:

```bash
docker-compose up
```

This command starts:

- Grafana Mimir
  - Authentication and multi-tenancy disabled (tenant ID is `anonymous`)
  - Highly-available configuration (three replicas)
- [Minio](https://min.io/)
  - S3-compatible persistent storage for blocks, rules, and alerts
- Prometheus
  - Scrapes Grafana Mimir metrics, then writes them back to Grafana Mimir to ensure availability of ingested metrics
- Grafana
  - Includes some Grafana Mimir dashboards preinstalled
- Load balancer
  - A simple nginx-based load balancer used to expose Grafana Mimir endpoints on the host

The following ports will be exposed on the host:

- Grafana on [`http://localhost:9000`](http://localhost:9000)
- Grafana Mimir on [`http://localhost:9009`](http://localhost:9009)

## Explore Grafana Mimir dashboards

Open Grafana running at [`http://localhost:9000`](http://localhost:9000) and check out some dashboards showing the status
and health of your Grafana Mimir local cluster. Metrics are queried directly from Grafana Mimir.

To begin with, we recommend looking at the following dashboards:

- [Writes](http://localhost:9000/d/0156f6d15aa234d452a33a4f13c838e3/mimir-writes)
- [Reads](http://localhost:9000/d/8d6ba60eccc4b6eedfa329b24b1bd339/mimir-reads)
- [Queries](http://localhost:9000/d/d9931b1054053c8b972d320774bb8f1d/mimir-queries)
- [Object Store](http://localhost:9000/d/d5a3a4489d57c733b5677fb55370a723/mimir-object-store)

Some notes:

- It may take few minutes after Grafana Mimir has been stated to have some meaningful metrics to display in the dashboards.
- Since in this setup Grafana Mimir is running without any ingress gateway, query-scheduler or memcached, the related panels are all expected to be empty.

To learn more about the Grafana configuration you can look at the [Mimir datasource](http://localhost:9000/datasources).

## Explore Grafana Mimir admin UI

1. Open Grafana Mimir admin UI at [`http://localhost:9009`](http://localhost:9009)
2. Open "[Ingester Ring Status](http://localhost:9009/ingester/ring)" to check the status of the hash ring used for series sharding and replication (you should see the 3 replicas correctly registered to the ring)
3. Open "[Memberlist Status](http://localhost:9009/memberlist)" to check the status and healthy of the Gossip-based clustering

To learn more about the Grafana Mimir configuration you can look at the content of `config/mimir.yaml`.

## Configure your first rules

The file `rules.yaml` contains an example recording rule and alerting rule we're going to configure in Grafana Mimir.
These rules will be evaluated by Mimir ruler: the resulting series of recording rule will be ingested by Grafana Mimir
itself, while the alerting rule will be notified to the Alertmanager.

1. Configure `mimirtool`
   ```bash
   export MIMIR_ADDRESS="http://localhost:9009"
   export MIMIR_TENANT_ID="anonymous"
   ```
1. Load example rules to Grafana Mimir
   ```bash
   mimirtool rules load rules.yaml
   ```
1. Check the configured rules via `mimirtool`
   ```bash
   mimirtool rules print
   ```
1. Check the configured rules via [Grafana Alerting UI](http://localhost:9000/alerting/list)
1. Query the resulting series from the recording rule using [Grafana Explore](http://localhost:9000/explore), which may require up to one minute to display after configuration.
   ```
   count:up
   ```
1. Check the alerts are correctly set to fire using the [Alertmanager UI](http://localhost:9009/alertmanager), which may require up to one minute to display after configuration.

## Summary

In this tutorial you locally run Grafana Mimir in a high-available setup, including a Prometheus instance remote writing
some metrics to Grafana Mimir and querying them via Mimir with Grafana. You also explored some of the dashboards released
by Grafana Mimir team and learned how to configure rules in Mimir.

Once done, you can release all Docker resources running:

```bash
docker-compose down
```
