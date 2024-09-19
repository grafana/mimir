---
aliases:
  - ../operators-guide/configure/configure-kafka-backend/
description: Learn how to configure Grafana Mimir to use Kafka for ingest storage.
menuTitle: Kafka
title: Configure the Grafana Mimir Kafka backend
weight: 66
---

# Configure the Grafana Mimir Kafka backend

Grafana Mimir supports using Kafka for the first layer of ingestion. This is an experimental feature released in Mimir 2.14.
This page is incomplete. It will be updated as the ingest storage feature matures and moves out of the experimental phase.

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
