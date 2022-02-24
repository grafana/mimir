---
title: "(Optional) Alertmanager"
description: "Overview of the alertmanager microservice."
weight: 20
---

# (Optional) Alertmanager

The Mimir Alertmanager adds multi-tenancy support and horizontal scalability on top of the [Prometheus Alertmanager](https://prometheus.io/docs/alerting/alertmanager/).
The Mimir Alertmanager is an **optional** component responsible for accepting alert notifications from the [Mimir ruler]({{<relref "./ruler.md">}}).
It deduplicates and groups alert notifications, and routes them to the correct notification channel, such as email, PagerDuty, or OpsGenie.

## Multi-tenancy

Multi-tenancy in the Mimir Alertmanager uses the tenant ID header as with all other Mimir components.
Each tenant has their own isolated alert routing configuration and Alertmanager UI.

### Tenant configurations

Each tenant has their Alertmanager configuration that defines notifications receivers and alerting routes.
The Mimir Alertmanager uses the same configuration file as the Prometheus Alertmanager.

> **Note:**
> The Mimir Alertmanager exposes the configuration API under the path set by the `-server.path-prefix` flag and not the path set by the `-http.alertmanager-http-prefix` flag.
> With the default configuration of `-server.path-prefix`, the Alertmanager URL used as the `mimirtool` `--address` flag has no path portion.

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

The Mimir Alertmanager has a number of per-tenant limits documented in [`limits`]({{<relref "../configuration/reference-configuration-parameters.md#limits" >}}).
Each Mimir Alertmanager limit configuration parameter has an `alertmanager` prefix.

## Alertmanager UI

The Mimir Alertmanager exposes the same web UI as the Prometheus Alertmanager at the `/alertmanager` endpoint.

When running Grafana Mimir with multi-tenancy enabled, the Alertmanager requires that any HTTP request includes the tenant ID header.
Tenants only see alerts sent to their Alertmanager.

For a complete reference of the tenant ID header and Alertmanager endpoints, refer to [HTTP API]({{<relref "../reference-http-api/_index.md" >}}).

The HTTP path prefix for the UI and HTTP API is configurable:

- `-http.alertmanager-http-prefix` configures the path prefix for Alertmanager endpoints.
- `-alertmanager.web.external-url` configures the source URLs generated in Alertmanager alerts and where to fetch web assets from.
  Unless you are using a reverse proxy in front of the Alertmanager API that rewrites routes, the path prefix set in `-alertmanager.web.external-url` should match the one set in `-http.alertmanager-http-prefix` (`/alertmanager` by default).
  Otherwise HTTP requests routing may not work as expected.

### Using a reverse proxy

When using a reverse proxy, ensure that you configure the HTTP path appropriately:

- Set `-http.alertmanager-http-prefix` to match the proxy path in your reverse proxy configuration.
- Set `-alertmanager.web.external-url` to the URL served by your reverse proxy.

## Horizontal scalability

[//]: # "TODO document the KV store backend and link it in this section"

### Sharding

To achieve horizontal scalability, the Mimir Alertmanager shards alerts by tenant.
Sharding requires that the number of Alertmanager replicas is greater-than or equal-to the replication factor configured by the `-alertmanager.sharding-ring.replication-factor` flag.

The Mimir Alertmanager replicas use the hash ring stored in the KV store to discover their peers.
This means that any Mimir Alertmanager replica can respond to any API or UI request for any tenant.
If the Mimir Alertmanager replica receiving the HTTP request doesn't own the tenant to which the request belongs to, the request is internally routed to the appropriate replica.
This mechanism is fully transparent to the final user.

> **Note:**
> When running with a single tenant, scaling the number of replicas to be greater than the replication factor offers no benefits as the Mimir Alertmanager shards by tenant and not individual alerts.

### State

The Mimir Alertmanager stores the alerts state on local disk at the location configured with `-alertmanager.storage.path`.

> **Warning:**
> When running the Mimir Alertmanager without replication, ensure persistence of the `-alertmanager.storage.path` directory to avoid losing alert state.

The Mimir Alertmanager also periodically stores the alerts state in the storage backend configured with `-alertmanager-storage.backend`.
Whenever an Alertmanager starts up it tries to load the alerts state for any given tenant from other Alertmanager replicas, falling back to the state periodically stored in the storage backend.
This fallback mechanism recovers previous state in the event of a cluster wide outage.
