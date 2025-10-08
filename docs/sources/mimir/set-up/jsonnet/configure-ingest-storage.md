---
aliases:
  - ../../operators-guide/deploy-grafana-mimir/jsonnet/configure-ingest-storage/
description: Learn how to configure Grafana Mimir ingest storage architecture when using Jsonnet.
menuTitle: Configure ingest storage architecture
title: Configure ingest storage architecture
weight: 20
---

# Configure ingest storage architecture

The [ingest storage](https://grafana.com/docs/mimir/<MIMIR_VERSION>/get-started/about-grafana-mimir-architecture/) is the next generation architecture of Grafana Mimir. It decouples the Mimir read and write paths using Apache Kafka or a Kafka-compatible backend.

To enable ingest storage, set the following Jsonnet:

```jsonnet
{
  _config+:: {
    ingest_storage_enabled: true,
  }
}
```

{{< admonition type="note" >}}
The ingest storage architecture requires a production-grade Apache Kafka cluster or Kafka-compatible backend.
{{< /admonition >}}

{{< admonition type="warning" >}}
The ingest storage architecture requires the ruler remote evaluation to be enabled for the Mimir cluster.
Refer to the [Configure the Grafana Mimir ruler with Jsonnet](https://grafana.com/docs/mimir/<MIMIR_VERSION>/set-up/jsonnet/configure-ruler/) documentation for details about the ruler's operational modes.
{{< /admonition >}}

## Kafka connection options

The Jsonnet configures some Kafka connection parameters by default:

- **Kafka topic**: `ingest`
- **Auto-created partitions**: 1000 (when the topic doesn't exist)
- **Kafka addresses**: `kafka.<namespace>.svc.<cluster_domain>:9092`

To customize the Kafka connection configuration, override the default settings:

```jsonnet
{
  _config+:: {
    ingest_storage_enabled: true,
  },

  // Override producer and consumer addresses for external Kafka
  ingest_storage_kafka_producer_address:: 'kafka-broker-1:9092,kafka-broker-2:9092',
  ingest_storage_kafka_consumer_address:: 'kafka-broker-1:9092,kafka-broker-2:9092',

  // Override Kafka client configuration
  ingest_storage_kafka_client_args+:: {
    'ingest-storage.kafka.topic': 'mimir-ingest',
    'ingest-storage.kafka.auto-create-topic-default-partitions': 500,
  },
}
```

### Kafka partition sizing

The configured topic must have at least as many partitions as the number of ingesters in one zone.

Refer to [Configure the Grafana Mimir Kafka backend](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configure-kafka-backend/) for more details about Kafka configurations.
