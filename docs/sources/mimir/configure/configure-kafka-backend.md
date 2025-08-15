---
aliases:
  - ../operators-guide/configure/configure-kafka-backend/
description: Learn how to configure Grafana Mimir to use Kafka for ingest storage.
menuTitle: Kafka
title: Configure the Grafana Mimir Kafka backend
weight: 130
---

# Configure the Grafana Mimir Kafka backend

Grafana Mimir supports using Kafka for the first layer of ingestion. This is an experimental feature released in Mimir 2.14.
This page is incomplete. It will be updated as the ingest storage feature matures and moves out of the experimental phase.

## Configuring ingest storage

The following configuration flags enable Grafana Mimir to use the ingest storage through Kafka backend:

- `-ingest-storage.enabled=true`<br />
  The ingest storage architecture must be explicitly enabled in all Mimir components.

- `-ingest-storage.kafka.address=<host:port>`<br />
  The `<host:port>` is the address of Kafka broker to bootstrap the connection.

- `-ingest-storage.kafka.topic=<name>`<br />
  The `<name>` is the name of the Kafka topic, that will be used for ingesting data.
- `-ingest-storage.kafka.auto-create-topic-default-partitions=<number>`<br />
  If the configured topic doesn't not exists in Kafka backend, the Mimir components (either consumers or producers),
  will create the topic on first access. The `<number>` parameter sets the number of partitions to create when the topic is automatically created. The number of partitions must be at least the number of ingesters in one zone.

Additionally, there configuration options are recommended, when Grafana Mimir runs in the ingest storage architecture:

- `-distributor.remote-timeout=5s`<br />
  Increase the default remote write timeout, that is applied to writing to Kafka too, because pushing
  to Kafka-compatible backends may be slower than writing to directly to ingesters.

Refer to Grafana Mimir [configuration parameters](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configuration-parameters/) for the detailed description of all available configuration options.

## Different Kafka backend implementations

Some Kafka-compatible implementations have different behavior for the Kafka API.
To set up Mimir to work with different Kafka backends, you need to configure some parameters.
Here are the Kafka flavors and additional configurations needed to set them up in Mimir.

### Apache Kafka

Use the default options with Apache Kafka. No additional configuration is needed.

### Confluent Kafka

Use the default options with Confluent Kafka. No additional configuration is needed.

### Warpstream

Configure the following CLI flags or their YAML equivalent.

```
-ingest-storage.kafka.use-compressed-bytes-as-fetch-max-bytes=false
```
