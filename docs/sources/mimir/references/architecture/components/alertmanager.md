---
aliases:
  - ../../../operators-guide/architecture/components/alertmanager/
description:
  The Alertmanager groups alert notifications and routes them to various
  notification channels.
menuTitle: (Optional) Alertmanager
title: (Optional) Grafana Mimir Alertmanager
weight: 100
---

# (Optional) Grafana Mimir Alertmanager

The Mimir Alertmanager adds multi-tenancy support and horizontal scalability to the [Prometheus Alertmanager](https://prometheus.io/docs/alerting/alertmanager/).
The Mimir Alertmanager is an optional component that accepts alert notifications from the [Mimir ruler]({{< relref "./ruler" >}}).
The Alertmanager deduplicates and groups alert notifications, and routes them to a notification channel, such as email, PagerDuty, or OpsGenie.

{{< admonition type="note" >}}
To run Mimir Alertmanager as a part of [monolithic deployment]({{< relref "../deployment-modes#monolithic-mode" >}}), run Mimir with the option `-target=all,alertmanager`.
{{< /admonition >}}

## Multi-tenancy

Like other Mimir components, multi-tenancy in the Mimir Alertmanager uses the tenant ID header.
Each tenant has an isolated alert routing configuration and Alertmanager UI.

### Tenant configurations

Each tenant has an Alertmanager configuration that defines notifications receivers and alerting routes.
The Mimir Alertmanager uses the same [configuration file](https://prometheus.io/docs/alerting/latest/configuration/#configuration-file) that the Prometheus Alertmanager uses.

{{< admonition type="note" >}}
The Mimir Alertmanager exposes the configuration API according to the path set by the `-server.path-prefix` flag.
It doesn't use the path set by the `-http.alertmanager-http-prefix` flag.

If you run Mimir with the default configuration, `-server.path-prefix`, where the default value is `/`, then only set the hostname for the `--address` flag of the `mimirtool` command; don't set a path-specific address.
For example, `/` is correct, and `/alertmanager` is incorrect.
{{< /admonition >}}

You can validate a configuration file using the `mimirtool` command:

```bash
mimirtool alertmanager verify <ALERTMANAGER CONFIGURATION FILE>
```

The following sample command shows how to upload a tenant's Alertmanager configuration using `mimirtool`:

```bash
mimirtool alertmanager load <ALERTMANAGER CONFIGURATION FILE>  \
  --address=<ALERTMANAGER URL>
  --id=<TENANT ID>
```

The following sample command shows how to retrieve a tenant's Alertmanager configuration using `mimirtool`:

```bash
mimirtool alertmanager get \
  --address=<ALERTMANAGER URL>
  --id=<TENANT ID>
```

The following sample commands shows how to delete a tenant's Alertmanager configuration using `mimirtool`:

```bash
mimirtool alertmanager delete \
  --address=<ALERTMANAGER URL>
  --id=<TENANT ID>
```

After the tenant uploads an Alertmanager configuration, the tenant can access the Alertmanager UI at the `/alertmanager` endpoint.

#### Fallback configuration

When a tenant doesn't have a Alertmanager configuration, the Grafana Mimir Alertmanager uses a fallback configuration.
By default, there is always a fallback configuration set.
You can overwrite the default fallback configuration via the `-alertmanager.configs.fallback` command-line flag.

{{< admonition type="warning" >}}
Without a fallback configuration or a tenant specific configuration, the Alertmanager UI is inaccessible and ruler notifications for that tenant fail.
{{< /admonition >}}

### Tenant limits

The Grafana Mimir Alertmanager has a number of per-tenant limits documented in [`limits`]({{< relref "../../../configure/configuration-parameters#limits" >}}).
Each Mimir Alertmanager limit configuration parameter has an `alertmanager` prefix.

## Alertmanager UI

The Mimir Alertmanager exposes the same web UI as the Prometheus Alertmanager at the `/alertmanager` endpoint.

When running Grafana Mimir with multi-tenancy enabled, the Alertmanager requires that any HTTP request include the tenant ID header.
Tenants only see alerts sent to their Alertmanager.

For a complete reference of the tenant ID header and Alertmanager endpoints, refer to [HTTP API]({{< relref "../../http-api" >}}).

You can configure the HTTP path prefix for the UI and the HTTP API:

- `-http.alertmanager-http-prefix` configures the path prefix for Alertmanager endpoints.
- `-alertmanager.web.external-url` configures the source URLs generated in Alertmanager alerts and from where to fetch web assets.

{{< admonition type="note" >}}
Unless you are using a reverse proxy in front of the Alertmanager API that rewrites routes, the path prefix set in `-alertmanager.web.external-url` must match the path prefix set in `-http.alertmanager-http-prefix` which is `/alertmanager` by default.

If the path prefixes don't match, HTTP requests routing might not work as expected.
{{< /admonition >}}

### Using a reverse proxy

When using a reverse proxy, use the following settings when you configure the HTTP path:

- Set `-http.alertmanager-http-prefix` to match the proxy path in your reverse proxy configuration.
- Set `-alertmanager.web.external-url` to the URL served by your reverse proxy.

## Templating

The Mimir Alertmanager adds some custom template functions to the default ones of the Prometheus Alertmanager.

| Function                | Params                                        | Description                                                                                                                                                                                                       |
| ----------------------- | --------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tenantID`              | -                                             | Returns ID of the tenant the alert belongs to.                                                                                                                                                                    |
| `queryFromGeneratorURL` | `generator_url`                               | Returns the URL decoded query from `GeneratorURL` of an alert set by a Prometheus. Example: `{{ queryFromGeneratorURL (index .Alerts 0).GeneratorURL }}`                                                          |
| `grafanaExploreURL`     | `grafana_URL`,`datasource`,`from`,`to`,`expr` | Returns link to Grafana explore with range query based on the input parameters. Example: `{{ grafanaExploreURL "https://foo.bar" "xyz" "now-12h" "now" (queryFromGeneratorURL (index .Alerts 0).GeneratorURL) }}` |

## Sharding and replication

The Alertmanager shards and replicates alerts by tenant.
Sharding requires that the number of Alertmanager replicas is greater-than or equal-to the replication factor configured by the `-alertmanager.sharding-ring.replication-factor` flag.

Grafana Mimir Alertmanager replicas use a [hash ring]({{< relref "../hash-ring" >}}) that is stored in the KV store to discover their peers.
This means that any Mimir Alertmanager replica can respond to any API or UI request for any tenant.
If the Mimir Alertmanager replica receiving the HTTP request doesn't own the tenant to which the request belongs, the request is internally routed to the appropriate replica.

To configure the Alertmanagers' hash ring, refer to [configuring hash rings]({{< relref "../../../configure/configure-hash-rings" >}}).

{{< admonition type="note" >}}
When running with a single tenant, scaling the number of replicas to be greater than the replication factor offers no benefits as the Mimir Alertmanager shards by tenant and not individual alerts.
{{< /admonition >}}

### State

The Mimir Alertmanager stores the alerts state on local disk at the location configured using `-alertmanager.storage.path`.

{{< admonition type="warning" >}}
When running the Mimir Alertmanager without replication, ensure persistence of the `-alertmanager.storage.path` directory to avoid losing alert state.
{{< /admonition >}}

The Mimir Alertmanager also periodically stores the alert state in the storage backend configured with `-alertmanager-storage.backend`.
When an Alertmanager starts, it attempts to load the alerts state for a given tenant from other Alertmanager replicas. If the load from other Alertmanager replicas fails, the Alertmanager falls back to the state that is periodically stored in the storage backend.

In the event of a cluster outage, this fallback mechanism recovers the backup of the previous state. Because backups are taken periodically, this fallback mechanism does not guarantee that the most recent state is restored.

## Ruler configuration

You must configure the [ruler]({{< relref "./ruler" >}}) with the addresses of Alertmanagers via the `-ruler.alertmanager-url` flag.

Point the address to Alertmanager’s API.
You can configure Alertmanager’s API prefix via the `-http.alertmanager-http-prefix` flag, which defaults to `/alertmanager`.
For example, if Alertmanager is listening at `http://mimir-alertmanager.namespace.svc.cluster.local` and it is using the default API prefix, set `-ruler.alertmanager-url` to `http://mimir-alertmanager.namespace.svc.cluster.local/alertmanager`.

## Enable UTF-8

In effort to support alerts from [OpenTelemetry](https://opentelemetry.io/) (OTel) data, [Prometheus Alertmanager](https://prometheus.io/docs/alerting/alertmanager/) has added support for UTF-8. This is supported as an opt-in feature for the Grafana Mimir Alertmanager in Mimir versions 2.12 and later.

{{< admonition type="warning" >}}
Enabling and then disabling UTF-8 strict mode can break existing tenant configurations if tenants added UTF-8 characters to their Alertmanager configuration while it was enabled. Once enabled, disable UTF-8 strict mode with caution.
{{< /admonition >}}

For new Mimir installations, enable support for UTF-8 before creating any tenant configurations. You can do this by changing [`utf8-strict-mode-enabled`]({{< relref "./../../../configure/configuration-parameters#alertmanager" >}}) to `true`.

For existing Mimir installations, there are a number of breaking changes that might affect existing tenant configurations. Follow these instructions to ensure all existing tenant configurations are compatible with UTF-8 before enabling it.

### What are the breaking changes?

In order to support UTF-8, Alertmanager has added a new parser for label matchers (often abbreviated as matchers), which has a number of breaking changes.

{{< admonition type="note" >}}
If you are unfamiliar with what matchers are or how they are used in a tenant configuration, you can find more information about them in the [Prometheus Alertmanager documentation](https://prometheus.io/docs/alerting/latest/configuration).
{{< /admonition >}}

Grafana Mimir provides a number of tools to help you identify whether any existing tenant configurations are affected by these breaking changes, and to migrate any affected tenant configurations in a way that is backwards-compatible, doesn't change the behavior of existing matchers, and works even in Mimir installations that do not have UTF-8 enabled.

### Identify affected tenant configurations

To identify affected tenant configurations, take the following steps:

1. Make sure Mimir is running version 2.12 or later.

1. Enable [`utf8-migration-logging-enabled`]({{< relref "./../../../configure/configuration-parameters#alertmanager" >}}) and set [`log_level`]({{< relref "./../../../configure/configuration-parameters#server" >}}) to `debug`. You must restart Mimir for the changes to take effect.

1. To identify any tenant configurations that are incompatible with UTF-8 (meaning the tenant configuration fails to load and the [fallback configuration](#fallback-configuration) is used instead), search Mimir server logs for lines containing `Alertmanager is moving to a new parser for labels and matchers, and this input is incompatible`. Each log line includes the invalid matcher from the tenant configuration and the ID of the affected tenant. For example:

   ```
   msg="Alertmanager is moving to a new parser for labels and matchers, and this input is incompatible. Alertmanager has instead parsed the input using the classic matchers parser as a fallback. To make this input compatible with the UTF-8 matchers parser please make sure all regular expressions and values are double-quoted. If you are still seeing this message please open an issue." input="foo=" err="end of input: expected label value" suggestion="foo=\"\"" user="1"
   ```

   In this example, the tenant with User ID `1` has an incompatible matcher in their tenant configuration `foo=` and should to be changed to the suggestion `foo=""`.

1. To identify any tenant configurations that are compatible with UTF-8 but contain matchers that might change in behavior when its enabled, search Mimir server logs for lines containing `Matchers input has disagreement`. Disagreement occurs when a matcher is valid, but due to adding support for UTF-8, it can behave differently when UTF-8 is enabled.

   ```
   msg="Matchers input has disagreement" input="foo=\"\\xf0\\x9f\\x99\\x82\"" user="1"
   ```

{{< admonition type="note" >}}
It is possible for a tenant configuration to be both incompatible with UTF-8 and have disagreement, as an individual tenant configuration can contain a large number of matchers across different routes and inhibition rules.
{{< /admonition >}}

### Fix tenant configurations

To fix any identified tenant configurations, take the following steps:

1. Use the `migrate-utf8` [command]({{< relref "./../../../manage/tools/mimirtool#migrate-alertmanager-configuration-for-utf-8-in-mimir-212-and-later" >}}) in mimirtool to fix any tenant configurations that are incompatible with UTF-8. This command can migrate existing tenant configurations in a way that is backwards-compatible, doesn't change the behavior of existing matchers, and works even in Mimir installations that don't have UTF-8 enabled. If you cannot use mimirtool, you can edit tenant configurations by hand through applying each suggestion from the Mimir server logs.

1. You must look at tenant configurations that have disagreement on a case-by-case basis. Depending on the nature of the disagreement, you might not need to fix a matcher with disagreement. For example `\xf0\x9f\x99\x82` is the byte sequence for the 🙂 emoji. If the intention is to match a literal 🙂 emoji then no change is required. However, if the intention is to match the literal `\xf0\x9f\x99\x82` then you need to change the matcher to use `\\xf0\\x9f\\x99\\x82` instead.

{{< admonition type="note" >}}
It's rare to find cases of disagreement in a tenant configuration, as most tenants do not need to match alerts that contain literal UTF-8 byte sequences in their labels.
{{< /admonition >}}

### Final steps

1. After identifying and fixing all affected tenant configurations, check the Mimir server logs again to make sure you haven't missed any tenant configurations.

1. To enable UTF-8, set [`utf8-strict-mode-enabled`]({{< relref "./../../../configure/configuration-parameters#alertmanager" >}}) to `true`. You must restart Mimir for the changes to take effect.

1. To confirm UTF-8 is enabled, search for `Starting Alertmanager in UTF-8 strict mode` in the Mimir server logs. If you find `Starting Alertmanager in classic mode` instead then UTF-8 is not enabled.

1. Any incompatible tenant configurations will fail to load. To identify if any tenant configurations are failing to load, search the Mimir server logs for lines containing `error applying config`, or query the `cortex_alertmanager_config_last_reload_successful` gauge for `0`.

1. You can disable [`utf8-migration-logging-enabled`]({{< relref "./../../../configure/configuration-parameters#alertmanager" >}}) and set [`log_level`]({{< relref "./../../../configure/configuration-parameters#server" >}}) back to its previous value.
