---
aliases:
  - ../operators-guide/configure/configure-kafka-backend/
description: Learn how to configure Grafana Mimir to use Kafka for ingest storage.
menuTitle: Kafka
title: Configure the Grafana Mimir Kafka backend
weight: 130
---

# Configure the Grafana Mimir Kafka backend

Grafana Mimir supports using Kafka as the first layer of ingestion in the ingest storage architecture. This configuration allows for scalable, decoupled ingestion that separates write and read paths to improve performance and resilience.

Starting with Grafana Mimir 3.0, ingest storage is the preferred and stable architecture for running Grafana Mimir.

## Configure ingest storage

Set the following configuration flags to enable Grafana Mimir to use ingest storage through a Kafka backend:

- `-ingest-storage.enabled=true`<br />
  You must explicitly enable the ingest storage architecture in all Mimir components.

- `-ingest-storage.kafka.address=<host:port>[,<host:port>...]`<br />
  The `<host:port>` is a Kafka seed broker address used to bootstrap the connection. You can configure a comma-separated list of seed broker addresses for higher bootstrap availability.

- `-ingest-storage.kafka.topic=<name>`<br />
  The `<name>` is the name of the Kafka topic that is used for ingesting data.
- `-ingest-storage.kafka.auto-create-topic-default-partitions=<number>`<br />
  If the configured topic doesn't exist in the Kafka backend, the Mimir components, either consumers or producers,
  create the topic on first access. The `<number>` parameter sets the number of partitions to create when the topic is automatically created. The number of partitions must be at least the number of ingesters in one zone.

Additionally, you can use these recommended configuration options when running Grafana Mimir with ingest storage architecture:

- `-distributor.remote-timeout=5s`<br />
  Use this setting to increase the default remote write timeout. This is recommended for writing to Kafka, because pushing
  to Kafka-compatible backends might be slower than writing directly to ingesters.

Refer to Grafana Mimir [configuration parameters](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configuration-parameters/) for detailed descriptions of all available configuration options.

## Different Kafka backend implementations

Some Kafka-compatible implementations have different behavior for the Kafka API.
To set up Mimir to work with different Kafka backends, you need to configure some parameters.
Here are the Kafka flavors and additional configurations needed to set them up in Mimir.

### Apache Kafka

In your Kafka broker configuration file (for example, `server.properties`), set the following property to support the default Mimir record size:

```
message.max.bytes=16000000
```

Mimir's default `-ingest-storage.kafka.producer-max-record-size-bytes` is approximately 15.2 MB.
Apache Kafka's default `message.max.bytes` is 1 MB.
Increase Kafka's `message.max.bytes` to at least `16000000` to match Mimir's maximum batch size; otherwise, Kafka rejects records larger than 1 MB.

To configure the limit at the topic level instead of the broker level, run:

```bash
bin/kafka-configs.sh --bootstrap-server <host:port> \
  --alter --entity-type topics --entity-name <topic-name> \
  --add-config max.message.bytes=16000000
```

### Confluent Kafka

In your Kafka broker configuration file (for example, `server.properties`), set the following property to support the default Mimir record size:

```
message.max.bytes=16000000
```

Mimir's default `-ingest-storage.kafka.producer-max-record-size-bytes` is approximately 15.2 MB.
Confluent Kafka's default `message.max.bytes` is 1 MB.
Increase Kafka's `message.max.bytes` to at least `16000000` to match Mimir's maximum batch size; otherwise, Kafka rejects records larger than 1 MB.

To configure the limit at the topic level instead of the broker level, run:

```bash
bin/kafka-configs.sh --bootstrap-server <host:port> \
  --alter --entity-type topics --entity-name <topic-name> \
  --add-config max.message.bytes=16000000
```

### Warpstream

Configure the following CLI flags or their YAML equivalent.

```
-ingest-storage.kafka.use-compressed-bytes-as-fetch-max-bytes=false
```
