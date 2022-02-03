---
title: "(Optional) Alertmanager"
description: "Overview of the alertmanager microservice."
weight: 20
---

# (Optional) Alertmanager

The Mimir Alertmanager adds multi-tenancy support and horizontal scalability on top of the [Prometheus Alertmanager](https://prometheus.io/docs/alerting/alertmanager/).
The Mimir Alertmanager is an **optional** service responsible for accepting alert notifications from the [Mimir ruler]({{<relref "./ruler.md">}}).
It deduplicates and groups alert notifications, and routes them to the correct notification channel, such as email, PagerDuty, or OpsGenie.


## Multi-tenancy

Multi-tenancy in the Mimir Alertmanager uses the tenant ID header as with all other Mimir services.
Each tenant has their own isoalted alert routing configuration and Alertmanager UI.

### Tenant configurations

Each tenant has their Alertmanager configuration that defines notifications receivers and alerting routes.
The Mimir Alertmanager uses the same configuration file as the Prometheus Alertmanager.

To upload a tenant's Alertmanager configuration using `mimirtool`:

```bash
mimirtool alertmanager load <ALERTMANAGER CONFIGURATION FILE>  \
  --address=<ALERTMANAGER URL>
  --id=<TENANT ID>
```

To retrieve a tenant's Alertmanager configuration using `mimirtool`:

```bash
mimirtool alertmanager get \
  --address=<ALERTMANAGER URL>
  --id=<TENANT ID>
```

To delete a tenant's Alertmanager configuration using `mimirtool`:

```bash
mimirtool alertmanager delete \
  --address=<ALERTMANAGER URL>
  --id=<TENANT ID>
```

Once the tenant has an uploaded Alertmanager configuration, they can access the Alertmanager UI at the URL `<ALERTMANAGER URL>/alertmanager`.

### Tenant limits

The Mimir Alertmanager has a number of per-tenant limits that are part of the [`limits_config`]({{<relref "../configuration/config-file-reference.md#limits_config" >}}).
Each Mimir Alertmanager limit configuration parameter has an `alertmanager` prefix.

## Horizontal scalability

To achieve horizontal scalability, the Mimir Alertmanager shards work on the tenant ID.
Where the Prometheus Alertmanager persists state to a local disk, the Mimir Alertmanager persists state to object storage accessible to each replica.
Replicas communicate state between each other to enforce rate limits and ensure the sending of alert notifications.
This means that users can access the Alertmanager UI, and rulers can send alert notifications to any Alertmanager replica.
