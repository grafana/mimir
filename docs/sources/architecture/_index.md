---
title: "About the architecture"
description: "Overview of the architecture of Grafana Mimir."
---

# About the architecture

Grafana Mimir has a service-based architecture.
The system has multiple horizontally scalable microservices that run separately and in parallel.

<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

![Architecture of Grafana Mimir](../images/architecture.png)

## Microservices

Most microservices are stateless and don't require any data persisted between process restarts.

Some microservices are stateful and rely on non-volatile storage to prevent data loss between process restarts.

A dedicate page describes each microservice in detail.

`{{< section >}}`
