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
Each tenant has their own isolated alert routing configuration and Alertmanager UI.

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

Once the tenant has an uploaded Alertmanager configuration, they can access the Alertmanager UI at the `/alertmanager` endpoint.

#### Fallback configuration

When a tenant doesn't have their own configuration, the Mimir Alertmanager uses a fallback configuration if configured.
By default, there is no fallback configuration set.
Specify a fallback configuration using the `-alertmanager.configs.fallback` command-line flag.

> **Warning**: Without a fallback configuration or a tenant specific configuration, the Alertmanager UI is inaccessible and ruler notifications for that tenant fail.

### Tenant limits

The Mimir Alertmanager has a number of per-tenant limits that are part of the [`limits_config`]({{<relref "../configuration/config-file-reference.md#limits_config" >}}).
Each Mimir Alertmanager limit configuration parameter has an `alertmanager` prefix.

## Alertmanager UI

The Mimir Alertmanager exposes the same web UI as the Prometheus Alertmanager at the `/alertmanager` endpoint.

When running Grafana Mimir with multi-tenancy enabled, the Alertmanager requires that any HTTP request includes the tenant ID header.
Tenants only see alerts sent to their Alertmanager.

For a complete reference of the tenant ID header and Alertmanager endpoints, refer to [HTTP API]({{<relref "../api/_index.md" >}}).

The HTTP path prefix for the UI and HTTP API is configurable:

- `-http.alertmanager-http-prefix` configures the path prefix for Alertmanager endpoints.
- `-alertmanager.web.external-url` configures the source URLs generated in Alertmanager alerts and where to fetch web assets from.

### Using a reverse proxy

When using a reverse proxy, ensure that you configure the HTTP path appropriately:

- Set `-http.alertmanager-http-prefix` to match the proxy path in your reverse proxy configuration.
- Set `-alertmanager.web.external-url` to the URL served by your reverse proxy.

## Horizontal scalability

[//]: # (TODO document the KV store backend and link it in this section)

### Sharding

To achieve horizontal scalability, the Mimir Alertmanager shards alerts by tenant.
To enable sharding, set `-alertmanager.sharding-enabled=true` and configure a KV store backend.
Sharding also requires that the number of Alertmanager replicas is greater-than or equal-to the replication factor configured by the `-alertmanager.sharding-ring.replication-factor` flag.
The Alertmanager replicas use the hash ring stored in the KV store to discover their peers.

### State

Where the Prometheus Alertmanager persists state to a local disk, the Mimir Alertmanager persists state to object storage accessible to each replica.
Replicas communicate state between each other to enforce rate limits and ensure the sending of alert notifications.
This means that users can access the Alertmanager UI, and rulers can send alert notifications to any Alertmanager replica.
