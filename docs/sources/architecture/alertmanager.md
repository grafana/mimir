---
title: "(Optional) Alertmanager"
description: "Overview of the alertmanager microservice."
weight: 80
---

# (Optional) Alertmanager

The **alertmanager** is an **optional service** responsible for accepting alert notifications from the [ruler]({{<relref "./ruler.md">}}), deduplicating and grouping them, and routing them to the correct notification channel, such as email, PagerDuty or OpsGenie.

The Mimir alertmanager is built on top of the [Prometheus Alertmanager](https://prometheus.io/docs/alerting/alertmanager/), adding multi-tenancy support. Like the [ruler]({{<relref "./ruler.md">}}), the alertmanager requires a database to store the per-tenant configuration.

Alertmanager is **semi-stateful**.
The Alertmanager persists information about silences and active alerts to its disk.
If all of the alertmanager nodes failed simultaneously there would be a loss of data.
