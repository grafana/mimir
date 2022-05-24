---
title: "Reference: Grafana Mimir HTTP API"
menuTitle: "Reference: HTTP API"
description: "Use the HTTP API to write and query time-series data and operate a Grafana Mimir cluster."
weight: 100
keywords:
  - Mimir API
  - Mimir endpoints
  - Mimir communication
  - Mimir querying
---

# Reference: Grafana Mimir HTTP API

Grafana Mimir exposes an HTTP API that you can use to write and query time series data, and operate the cluster.

This document groups API endpoints by service. Note that the API endpoints are exposed when you run Grafana Mimir in microservices mode and monolithic mode:

- **Microservices mode**: Each service exposes its own endpoints.
- **Monolithic mode**: The Grafana Mimir instance exposes all API endpoints.

## Endpoints

| API                                                                                   | Service                 | Endpoint                                                                  |
| ------------------------------------------------------------------------------------- | ----------------------- | ------------------------------------------------------------------------- |
| [Index page](#index-page)                                                             | _All services_          | `GET /`                                                                   |
| [Configuration](#configuration)                                                       | _All services_          | `GET /config`                                                             |
| [Runtime Configuration](#runtime-configuration)                                       | _All services_          | `GET /runtime_config`                                                     |
| [Services' status](#services-status)                                                  | _All services_          | `GET /services`                                                           |
| [Readiness probe](#readiness-probe)                                                   | _All services_          | `GET /ready`                                                              |
| [Metrics](#metrics)                                                                   | _All services_          | `GET /metrics`                                                            |
| [Pprof](#pprof)                                                                       | _All services_          | `GET /debug/pprof`                                                        |
| [Fgprof](#fgprof)                                                                     | _All services_          | `GET /debug/fgprof`                                                       |
| [Build information](#build-information)                                               | _All services_          | `GET /api/v1/status/buildinfo`                                            |
| [Remote write](#remote-write)                                                         | Distributor             | `POST /api/v1/push`                                                       |
| [Tenants stats](#tenants-stats)                                                       | Distributor             | `GET /distributor/all_user_stats`                                         |
| [HA tracker status](#ha-tracker-status)                                               | Distributor             | `GET /distributor/ha_tracker`                                             |
| [Flush chunks / blocks](#flush-chunks--blocks)                                        | Ingester                | `GET,POST /ingester/flush`                                                |
| [Shutdown](#shutdown)                                                                 | Ingester                | `GET,POST /ingester/shutdown`                                             |
| [Ingesters ring status](#ingesters-ring-status)                                       | Distributor,Ingester    | `GET /ingester/ring`                                                      |
| [Instant query](#instant-query)                                                       | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query`                          |
| [Range query](#range-query)                                                           | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query_range`                    |
| [Exemplar query](#exemplar-query)                                                     | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query_exemplars`                |
| [Get series by label matchers](#get-series-by-label-matchers)                         | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/series`                         |
| [Get label names](#get-label-names)                                                   | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/labels`                         |
| [Get label values](#get-label-values)                                                 | Querier, Query-frontend | `GET <prometheus-http-prefix>/api/v1/label/{name}/values`                 |
| [Get metric metadata](#get-metric-metadata)                                           | Querier, Query-frontend | `GET <prometheus-http-prefix>/api/v1/metadata`                            |
| [Remote read](#remote-read)                                                           | Querier, Query-frontend | `POST <prometheus-http-prefix>/api/v1/read`                               |
| [Label names cardinality](#label-names-cardinality)                                   | Querier, Query-frontend | `GET, POST <prometheus-http-prefix>/api/v1/cardinality/label_names`       |
| [Label values cardinality](#label-values-cardinality)                                 | Querier, Query-frontend | `GET, POST <prometheus-http-prefix>/api/v1/cardinality/label_values`      |
| [Build information](#build-information)                                               | Querier, Query-frontend | `GET <prometheus-http-prefix>/api/v1/status/buildinfo`                    |
| [Get tenant ingestion stats](#get-tenant-ingestion-stats)                             | Querier                 | `GET /api/v1/user_stats`                                                  |
| [Ruler ring status](#ruler-ring-status)                                               | Ruler                   | `GET /ruler/ring`                                                         |
| [Ruler rules ](#ruler-rules)                                                          | Ruler                   | `GET /ruler/rule_groups`                                                  |
| [List Prometheus rules](#list-prometheus-rules)                                       | Ruler                   | `GET <prometheus-http-prefix>/api/v1/rules`                               |
| [List Prometheus alerts](#list-prometheus-alerts)                                     | Ruler                   | `GET <prometheus-http-prefix>/api/v1/alerts`                              |
| [List rule groups](#list-rule-groups)                                                 | Ruler                   | `GET <prometheus-http-prefix>/config/v1/rules`                            |
| [Get rule groups by namespace](#get-rule-groups-by-namespace)                         | Ruler                   | `GET <prometheus-http-prefix>/config/v1/rules/{namespace}`                |
| [Get rule group](#get-rule-group)                                                     | Ruler                   | `GET <prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}`    |
| [Set rule group](#set-rule-group)                                                     | Ruler                   | `POST <prometheus-http-prefix>/config/v1/rules/{namespace}`               |
| [Delete rule group](#delete-rule-group)                                               | Ruler                   | `DELETE <prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}` |
| [Delete namespace](#delete-namespace)                                                 | Ruler                   | `DELETE <prometheus-http-prefix>/config/v1/rules/{namespace}`             |
| [Delete tenant configuration](#delete-tenant-configuration)                           | Ruler                   | `POST /ruler/delete_tenant_config`                                        |
| [Alertmanager status](#alertmanager-status)                                           | Alertmanager            | `GET /multitenant_alertmanager/status`                                    |
| [Alertmanager configs](#alertmanager-configs)                                         | Alertmanager            | `GET /multitenant_alertmanager/configs`                                   |
| [Alertmanager ring status](#alertmanager-ring-status)                                 | Alertmanager            | `GET /multitenant_alertmanager/ring`                                      |
| [Alertmanager UI](#alertmanager-ui)                                                   | Alertmanager            | `GET <alertmanager-http-prefix>`                                          |
| [Build Information](#build-information)                                               | Alertmanager            | `GET <alertmanager-http-prefix>/api/v1/status/buildinfo`                  |
| [Alertmanager Delete Tenant Configuration](#alertmanager-delete-tenant-configuration) | Alertmanager            | `POST /multitenant_alertmanager/delete_tenant_config`                     |
| [Get Alertmanager configuration](#get-alertmanager-configuration)                     | Alertmanager            | `GET /api/v1/alerts`                                                      |
| [Set Alertmanager configuration](#set-alertmanager-configuration)                     | Alertmanager            | `POST /api/v1/alerts`                                                     |
| [Delete Alertmanager configuration](#delete-alertmanager-configuration)               | Alertmanager            | `DELETE /api/v1/alerts`                                                   |
| [Tenant delete request](#tenant-delete-request)                                       | Purger                  | `POST /purger/delete_tenant`                                              |
| [Tenant delete status](#tenant-delete-status)                                         | Purger                  | `GET /purger/delete_tenant_status`                                        |
| [Store-gateway ring status](#store-gateway-ring-status)                               | Store-gateway           | `GET /store-gateway/ring`                                                 |
| [Store-gateway tenants](#store-gateway-tenants)                                       | Store-gateway           | `GET /store-gateway/tenants`                                              |
| [Store-gateway tenant blocks](#store-gateway-tenant-blocks)                           | Store-gateway           | `GET /store-gateway/tenant/{tenant}/blocks`                               |
| [Compactor ring status](#compactor-ring-status)                                       | Compactor               | `GET /compactor/ring`                                                     |

### Path prefixes

The following table provides usage of placeholder path prefixes, for prefixes that are configurable.

| Prefix                       | Default         | CLI flag                         | YAML configuration               |
| ---------------------------- | --------------- | -------------------------------- | -------------------------------- |
| `<prometheus-http-prefix>`   | `/prometheus`   | `-http.prometheus-http-prefix`   | `api > prometheus_http_prefix`   |
| `<alertmanager-http-prefix>` | `/alertmanager` | `-http.alertmanager-http-prefix` | `api > alertmanager_http_prefix` |

### Authentication

Endpoints that require authentication must be called with the `X-Scope-OrgID` HTTP request header specified to the tenant ID.

If you disable multi-tenancy, Grafana Mimir doesn't require any request to include the `X-Scope-OrgID` header.

Multi-tenancy can be enabled and disabled via the `-auth.multitenancy-enabled` flag or its respective YAML configuration option.

For more information about authentication and authorization, refer to [Authentication and Authorization]({{< relref "../securing/authentication-and-authorization.md" >}}).

## All services

The following API endpoints are exposed by all services.

### Index page

```
GET /
```

This endpoint displays an index page with links to other web pages exposed by Grafana Mimir.

### Configuration

```
GET /config
```

This endpoint displays the configuration currently applied to Grafana Mimir including default values and settings via CLI flags. This endpoint provides the configuration in YAML format and masks sensitive data.

> **Note**: The exported configuration doesn't include the per-tenant overrides.

#### Different modes

```
GET /config?mode=diff
```

This endpoint displays the differences between the Grafana Mimir default configuration and the current configuration.

```
GET /config?mode=defaults
```

This endpoint displays the default configuration values.

### Runtime Configuration

```
GET /runtime_config
```

This endpoint displays the [runtime configuration]({{< relref "../configuring/about-runtime-configuration.md" >}}) currently applied to Grafana Mimir, in YAML format, including default values.
The endpoint is only available if Grafana Mimir is configured with the `-runtime-config.file` option.

#### Different modes

```
GET /runtime_config?mode=diff
```

This endpoint displays the differences between the Grafana Mimir default runtime configuration and the current runtime configuration.

### Services' status

```
GET /services
```

This endpoint displays a web page with the status of internal Grafana Mimir services.

### Readiness probe

```
GET /ready
```

This endoint returns 200 when Grafana Mimir is ready to serve traffic.

### Metrics

```
GET /metrics
```

This endpoint returns the metrics for the running Grafana Mimir service in the Prometheus exposition format.

### Pprof

```
GET /debug/pprof/heap
GET /debug/pprof/block
GET /debug/pprof/profile
GET /debug/pprof/trace
GET /debug/pprof/goroutine
GET /debug/pprof/mutex
```

These endpoints return runtime profiling data in the format expected by the pprof visualization tool. There are many things that can be profiled using this endpoint, including heap, trace, goroutine, and so on.

For more information about pprof, refer to [pprof](https://golang.org/pkg/net/http/pprof/).

### Fgprof

```
GET /debug/fgprof
```

This endpoint returns the sampling Go profiling data that you can use to analyze On-CPU and Off-CPU (for example, I/O) time.

For more information about fgprof, refer to [fgprof](https://github.com/felixge/fgprof).

### Build information

```
GET /api/v1/status/buildinfo
GET <prometheus-http-prefix>/api/v1/status/buildinfo
GET <alertmanager-http-prefix>/api/v1/status/buildinfo
```

This endpoint returns in JSON format information about the build and enabled features. The format returned is not identical, but is similar to the [Prometheus Build Information endpoint](https://prometheus.io/docs/prometheus/latest/querying/api/#build-information).

## Distributor

The following endpoints relate to the [distributor]({{< relref "../architecture/components/distributor.md" >}}).

### Remote write

```
POST /api/v1/push
```

Entrypoint for the [Prometheus remote write](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

This endpoint accepts an HTTP POST request with a body that contains a request encoded with [Protocol Buffers](https://developers.google.com/protocol-buffers) and compressed with [Snappy](https://github.com/google/snappy).
You can find the definition of the protobuf message in [pkg/mimirpb/mimir.proto](https://github.com/grafana/mimir/blob/main/pkg/mimirpb/mimir.proto).
The HTTP request must contain the header `X-Prometheus-Remote-Write-Version` set to `0.1.0`.

To skip the label name validation, perform the following actions:

- Enable API's flag `-api.skip-label-name-validation-header-enabled=true`
- Ensure that the request is sent with the header `X-Mimir-SkipLabelNameValidation: true`

This feature supports the writes from non-standard downstream clients that have metric name not Prometheus compliant.

For more information, refer to Prometheus [Remote storage integrations](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations).

Requires [authentication](#authentication).

### Distributor ring status

```
GET /distributor/ring
```

This endpoint displays a web page with the distributor hash ring status, including the state, and the health and last heartbeat time of each distributor.

### Tenants stats

```
GET /distributor/all_user_stats
```

This endpoint displays a web page that shows per-tenant statistics updated in real time, including the total number of active series across all ingesters and the current ingestion rate displayed in samples per second.

> **Note:** This endpoint requires all ingesters to be `ACTIVE` in the ring for a successful response.

### HA tracker status

```
GET /distributor/ha_tracker
```

This endpoint displays a web page with the current status of the HA tracker, including the elected replica for each Prometheus HA cluster.

## Ingester

The following endpoints relate to the [ingester]({{< relref "../architecture/components/ingester.md" >}}).

### Flush chunks / blocks

```
GET,POST /ingester/flush
```

This endpoint triggers a flush of the in-memory series time series data to the long-term storage.
This endpoint also triggers the flush when `-blocks-storage.tsdb.flush-blocks-on-shutdown` is disabled.

This endpoint accepts a `tenant` parameter to specify the tenant whose blocks are compacted and shipped.
This parameter might be specified multiple times to select more tenants.
If no tenant is specified, all tenants are flushed.

The flush endpoint also accepts a `wait=true` parameter, which makes the call synchronous, and only returns a status code after flushing completes.

> **Note**: The returned status code does not reflect the result of flush operation.

### Shutdown

```
GET,POST /ingester/shutdown
```

This endpoint flushes in-memory time series data from ingesters to the long-term storage, and then shuts down the ingester service.
After the shutdown endpoint returns, the operator or any automation that's used terminates the process with a `SIGINT` / `SIGTERM` signal.
During this time, `/ready` does not return 200.
This endpoint unregisters the ingester from the ring even if you disable `-ingester.ring.unregister-on-shutdown`.

This API endpoint is usually used by scale down automations.

### Ingesters ring status

```
GET /ingester/ring
```

This endpoint displays a web page with the ingesters hash ring status, including the state, health, and last heartbeat time of each ingester.

## Querier / Query-frontend

The following endpoints are exposed both by the [querier]({{< relref "../architecture/components/querier.md" >}}) and [query-frontend]({{< relref "../architecture/components/query-frontend/index.md" >}}).

### Instant query

```
GET,POST <prometheus-http-prefix>/api/v1/query
```

This endpoint is compatible with the Prometheus instant query endpoint.

For more information about Prometheus instant queries, refer to Prometheus [instant query](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries).

Requires [authentication](#authentication).

### Range query

```
GET,POST <prometheus-http-prefix>/api/v1/query_range
```

This endpoint is compatible with the Prometheus range query endpoint. When a client sends a request through the query-frontend, the query-frontend uses caching and execution parallelization to accelerate the query.

For more information about Prometheus range queries, refer to Prometheus [range query](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries).

Requires [authentication](#authentication).

### Exemplar query

```
GET,POST <prometheus-http-prefix>/api/v1/query_exemplars
```

This endpoint is compatible with the Prometheus exemplar query endpoint.

For more information about Prometheus exemplar queries, refer to Prometheus [exemplar query](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-exemplars).

Requires [authentication](#authentication).

### Get series by label matchers

```
GET,POST <prometheus-http-prefix>/api/v1/series
```

For more information, refer to Prometheus [series endpoint](https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers).

Requires [authentication](#authentication).

### Get label names

```
GET,POST <prometheus-http-prefix>/api/v1/labels
```

For more information, refer to Prometheus [get label names](https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names).

Requires [authentication](#authentication).

### Get label values

```
GET <prometheus-http-prefix>/api/v1/label/{name}/values
```

For more information, refer to Prometheus [get label values](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values).

Requires [authentication](#authentication).

### Get metric metadata

```
GET <prometheus-http-prefix>/api/v1/metadata
```

Prometheus-compatible metric metadata endpoint.

For more information, refer to Prometheus [metric metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metric-metadata).

Requires [authentication](#authentication).

### Remote read

```
POST <prometheus-http-prefix>/api/v1/read
```

Prometheus-compatible [remote read](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read) endpoint.

For more information, refer to Prometheus [Remote storage integrations](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations).

Requires [authentication](#authentication).

### Label names cardinality

```
GET,POST <prometheus-http-prefix>/api/v1/cardinality/label_names
```

Returns realtime label names cardinality across all ingesters, for the authenticated tenant, in `JSON` format.
It counts distinct label values per label name.

As far as this endpoint generates cardinality report using only values from currently opened TSDBs in ingesters, two subsequent calls may return completely different results, if ingester did a block
cutting between the calls.

The items in the field `cardinality` are sorted by `label_values_count` in DESC order and by `label_name` in ASC order.

The count of items is limited by `limit` request param.

This endpoint is disabled by default and can be enabled via the `-querier.cardinality-analysis-enabled` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

#### Request params

- **selector** - _optional_ - specifies PromQL selector that will be used to filter series that must be analyzed.
- **limit** - _optional_ - specifies max count of items in field `cardinality` in response (default=20, min=0, max=500)

#### Response schema

```json
{
  "label_values_count_total": <number>,
  "label_names_count": <number>,
  "cardinality": [
    {
      "label_name": <string>,
      "label_values_count": <number>
    }
  ]
}
```

### Label values cardinality

```
GET,POST <prometheus-http-prefix>/api/v1/cardinality/label_values
```

Returns realtime label values cardinality associated to request param `label_names[]` across all ingesters, for the authenticated tenant, in `JSON` format.
It returns the series count per label value associated to request param `label_names[]`.

As far as this endpoint generates cardinality report using only values from currently opened TSDBs in ingesters, two subsequent calls may return completely different results, if ingester did a block
cutting between the calls.

The items in the field `labels` are sorted by `series_count` in DESC order and by `label_name` in ASC order.
The items in the field `cardinality` are sorted by `series_count` in DESC order and by `label_value` in ASC order.

The count of `cardinality` items is limited by request param `limit`.

This endpoint is disabled by default and can be enabled via the `-querier.cardinality-analysis-enabled` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

#### Request params

- **label_names[]** - _required_ - specifies labels for which cardinality must be provided.
- **selector** - _optional_ - specifies PromQL selector that will be used to filter series that must be analyzed.
- **limit** - _optional_ - specifies max count of items in field `cardinality` in response (default=20, min=0, max=500).

#### Response schema

```json
{
  "series_count_total": <number>,
  "labels": [
    {
      "label_name": <string>,
      "label_values_count": <number>,
      "series_count": <number>,
      "cardinality": [
        {
          "label_value": <string>,
          "series_count": <number>
        }
      ]
    }
  ]
}
```

- **series_count_total** - total number of series across opened TSDBs in all ingesters
- **labels[].label_name** - label name requested via the request param `label_names[]`
- **labels[].label_values_count** - total number of label values for the label name (note that dependent on the `limit` request param it is possible that not all label values are present in `cardinality`)
- **labels[].series_count** - total number of series having `labels[].label_name`
- **labels[].cardinality[].label_value** - label value associated to `labels[].label_name`
- **labels[].cardinality[].series_count** - total number of series having `label_value` for `label_name`

## Querier

### Get tenant ingestion stats

```
GET /api/v1/user_stats
```

Returns realtime ingestion rate, for the authenticated tenant, in `JSON` format.

Requires [authentication](#authentication).

## Ruler

The ruler API endpoints require to configure a backend object storage to store the recording rules and alerts. The ruler API uses the concept of a "namespace" when creating rule groups. This is a stand in for the name of the rule file in Prometheus and rule groups must be named uniquely within a namespace.

### Ruler ring status

```
GET /ruler/ring
```

Displays a web page with the ruler hash ring status, including the state, healthy and last heartbeat time of each ruler.

### Ruler rules

```
GET /ruler/rule_groups
```

List all tenant rules. This endpoint is not part of ruler-API and is always available regardless of whether ruler-API is enabled or not. It should not be exposed to end users. This endpoint returns a YAML dictionary with all the rule groups for each tenant and `200` status code on success.

### List Prometheus rules

```
GET <prometheus-http-prefix>/api/v1/rules
```

Prometheus-compatible rules endpoint to list alerting and recording rules that are currently loaded.

For more information, refer to Prometheus [rules](https://prometheus.io/docs/prometheus/latest/querying/api/#rules).

Requires [authentication](#authentication).

### List Prometheus alerts

```
GET <prometheus-http-prefix>/api/v1/alerts
```

Prometheus-compatible rules endpoint to list of all active alerts.

For more information, refer to Prometheus [alerts](https://prometheus.io/docs/prometheus/latest/querying/api/#alerts) documentation.

Requires [authentication](#authentication).

### List rule groups

```
GET <prometheus-http-prefix>/config/v1/rules

# Deprecated; will be removed in Mimir v2.2.0
GET /api/v1/rules

# Deprecated; will be removed in Mimir v2.2.0
GET <prometheus-http-prefix>/rules
```

List all rules configured for the authenticated tenant. This endpoint returns a YAML dictionary with all the rule groups for each namespace and `200` status code on success.

This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

**Example response**

```yaml
---
<namespace1>:
- name: <string>
  interval: <duration;optional>
  source_tenants:
    - <string>
  rules:
  - record: <string>
      expr: <string>
  - alert: <string>
      expr: <string>
      for: <duration>
      annotations:
        <annotation_name>: <string>
      labels:
        <label_name>: <string>
- name: <string>
  interval: <duration;optional>
  source_tenants:
    - <string>
  rules:
  - record: <string>
      expr: <string>
  - alert: <string>
      expr: <string>
      for: <duration>
      annotations:
        <annotation_name>: <string>
      labels:
        <label_name>: <string>
<namespace2>:
- name: <string>
  interval: <duration;optional>
  source_tenants:
    - <string>
  rules:
  - record: <string>
      expr: <string>
  - alert: <string>
      expr: <string>
      for: <duration>
      annotations:
        <annotation_name>: <string>
      labels:
        <label_name>: <string>
```

### Get rule groups by namespace

```
GET <prometheus-http-prefix>/config/v1/rules/{namespace}

# Deprecated; will be removed in Mimir v2.2.0
GET /api/v1/rules/{namespace}

# Deprecated; will be removed in Mimir v2.2.0
GET <prometheus-http-prefix>/rules/{namespace}
```

Returns the rule groups defined for a given namespace.

This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

**Example response**

```yaml
name: <string>
interval: <duration;optional>
source_tenants:
  - <string>
rules:
  - record: <string>
    expr: <string>
  - alert: <string>
    expr: <string>
    for: <duration>
    annotations:
      <annotation_name>: <string>
    labels:
      <label_name>: <string>
```

### Get rule group

```
GET <prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}

# Deprecated; will be removed in Mimir v2.2.0
GET /api/v1/rules/{namespace}/{groupName}

# Deprecated; will be removed in Mimir v2.2.0
GET <prometheus-http-prefix>/rules/{namespace}/{groupName}
```

Returns the rule group matching the request namespace and group name.

This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

### Set rule group

```
POST /<prometheus-http-prefix>/config/v1/rules/{namespace}

# Deprecated; will be removed in Mimir v2.2.0
POST /api/v1/rules/{namespace}

# Deprecated; will be removed in Mimir v2.2.0
POST <prometheus-http-prefix>/rules/{namespace}
```

Creates or updates a rule group.
This endpoint expects a request with `Content-Type: application/yaml` header and the rules group **YAML** definition in the request body, and returns `202` on success.
The request body must contain the definition of one and only one rule group.

This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

> **Note:** When using `curl` send the request body from a file, ensure that you use the `--data-binary` flag instead of `-d`, `--data`, or `--data-ascii`.
> The latter options do not preserve carriage returns and newlines.

#### Example request body

```yaml
name: MyGroupName
rules:
  - alert: MyAlertName
    expr: up == 0
    labels:
      severity: warning
```

#### Federated rule groups

A federated rule groups is a rule group with a non-empty `source_tenants`.

The `source_tenants` field allows aggregating data from multiple tenants while evaluating a rule group. The expressions
of each rule in the group will be evaluated against the data of all tenants in `source_tenants`. If `source_tenants` is
empty or omitted, then the tenant under which the group is created will be treated as the `source_tenant`.

Federated rule groups are skipped during evaluation by default. This feature depends on
the cross-tenant query federation feature. To enable federated rules
set `-ruler.tenant-federation.enabled=true` and `-tenant-federation.enabled=true` CLI flags (or their respective YAML
config options).

During evaluation query limits applied to single tenants are also applied to each query in the rule group. For example,
if `tenant-a` has a federated rule group with `source_tenants: [tenant-b, tenant-c]`, then query limits for `tenant-b`
and `tenant-c` will be applied. If any of these limits is exceeded, the whole evaluation will fail. No partial results
will be saved. The same "no partial results" guarantee applies to queries failing for other reasons (e.g. ingester
unavailability).

The time series used during evaluation of federated rules will have the `__tenant_id__` label, similar to how it is
present on series returned with cross-tenant query federation.

**Considerations:** Federated rule groups allow data from multiple source tenants to be written into a single
destination tenant. This makes the existing separation of tenants' data less clear. For example, `tenant-a` has a
federated rule group that aggregates over `tenant-b`'s data (e.g. `sum(metric_b)`) and writes the result back
into `tenant-a`'s storage (e.g. as metric `sum:metric_b`). Now part of `tenant-b`'s data is copied to `tenant-a` (albeit
aggregated). Have this in mind when configuring the access control layer in front of mimir and when enabling federated
rules via `-ruler.tenant-federation.enabled`.

#### Example "federated rules group" request body

```yaml
name: <string>
interval: <duration;optional>
source_tenants:
  - <string>
rules:
  - record: <string>
    expr: <string>
  - alert: <string>
    expr: <string>
    for: <duration>
    annotations:
      <annotation_name>: <string>
    labels:
      <label_name>: <string>
```

### Delete rule group

```
DELETE /<prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}

# Deprecated; will be removed in Mimir v2.2.0
DELETE /api/v1/rules/{namespace}/{groupName}

# Deprecated; will be removed in Mimir v2.2.0
DELETE <prometheus-http-prefix>/rules/{namespace}/{groupName}
```

Deletes a rule group by namespace and group name. This endpoints returns `202` on success.

This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

### Delete namespace

```
DELETE /<prometheus-http-prefix>/config/v1/rules/{namespace}

# Deprecated; will be removed in Mimir v2.2.0
DELETE /api/v1/rules/{namespace}

# Deprecated; will be removed in Mimir v2.2.0
DELETE <prometheus-http-prefix>/rules/{namespace}
```

Deletes all the rule groups in a namespace (including the namespace itself). This endpoint returns `202` on success.

This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

### Delete tenant configuration

```
POST /ruler/delete_tenant_config
```

This deletes all rule groups for a tenant, and returns `200` on success. Calling this endpoint when no rule groups exist for a tenant returns `200`. Authentication is only to identify the tenant.

This is intended as internal API, and not to be exposed to users. This endpoint is enabled regardless of whether `-ruler.enable-api` is enabled or not.

Requires [authentication](#authentication).

## Alertmanager

### Alertmanager status

```
GET /multitenant_alertmanager/status
```

Displays a web page with the current status of the Alertmanager, including the Alertmanager cluster members.

### Alertmanager configs

```
GET /multitenant_alertmanager/configs
```

List all Alertmanager configurations. This endpoint is not part of Alertmanager API and is always available regardless of whether Alertmanager API is enabled or not. It should not be exposed to end users. This endpoint returns a YAML dictionary with all the Alertmanager configurations and `200` status code on success.

### Alertmanager ring status

```
GET /multitenant_alertmanager/ring
```

Displays a web page with the Alertmanager hash ring status, including the state, healthy and last heartbeat time of each Alertmanager instance.

### Alertmanager UI

```
GET /<alertmanager-http-prefix>
```

Displays the Alertmanager UI.

Requires [authentication](#authentication).

### Alertmanager Delete Tenant Configuration

```
POST /multitenant_alertmanager/delete_tenant_config
```

This endpoint deletes configuration for a tenant identified by `X-Scope-OrgID` header.
It is internal, available even if Alertmanager API is disabled.
The endpoint returns a status code of `200` if the user's configuration has been deleted, or it didn't exist in the first place.

Requires [authentication](#authentication).

### Get Alertmanager configuration

```
GET /api/v1/alerts
```

Get the current Alertmanager configuration for the authenticated tenant, reading it from the configured object storage.

This endpoint doesn't accept any URL query parameter and returns `200` on success.

This endpoint can disabled enabled via the `-alertmanager.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

### Set Alertmanager configuration

```
POST /api/v1/alerts
```

Stores or updates the Alertmanager configuration for the authenticated tenant. The Alertmanager configuration is stored in the configured backend object storage.

This endpoint expects the Alertmanager **YAML** configuration in the request body and returns `201` on success.

This endpoint can disabled enabled via the `-alertmanager.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

> **Note:** When using `curl` send the request body from a file, ensure that you use the `--data-binary` flag instead of `-d`, `--data`, or `--data-ascii`.
> The latter options do not preserve carriage returns and newlines.

#### Example request body

```yaml
template_files:
  default_template: |
    {{ define "__alertmanager" }}AlertManager{{ end }}
    {{ define "__alertmanagerURL" }}{{ .ExternalURL }}/#/alerts?receiver={{ .Receiver | urlquery }}{{ end }}
alertmanager_config: |
  global:
    smtp_smarthost: 'localhost:25'
    smtp_from: 'youraddress@example.org'
  templates:
    - 'default_template'
  route:
    receiver: example-email
  receivers:
    - name: example-email
      email_configs:
      - to: 'youraddress@example.org'
```

### Delete Alertmanager configuration

```
DELETE /api/v1/alerts
```

Deletes the Alertmanager configuration for the authenticated tenant.

This endpoint doesn't accept any URL query parameter and returns `200` on success.

This endpoint can be disabled via the `-alertmanager.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

## Purger

The Purger service provides APIs for requesting tenant deletion.

### Tenant Delete Request

```
POST /purger/delete_tenant
```

Request deletion of ALL tenant data. Experimental.

Requires [authentication](#authentication).

### Tenant Delete Status

```
GET /purger/delete_tenant_status
```

Returns status of tenant deletion. Output format to be defined. Experimental.

Requires [authentication](#authentication).

## Store-gateway

### Store-gateway ring status

```
GET /store-gateway/ring
```

Displays a web page with the store-gateway hash ring status, including the state, healthy and last heartbeat time of each store-gateway.

### Store-gateway tenants

```
GET /store-gateway/tenants
```

Displays a web page with the list of tenants with blocks in the storage configured for store-gateway.

### Store-gateway tenant blocks

```
GET /store-gateway/tenant/{tenant}/blocks
```

Displays a web page listing the blocks for a given tenant.

## Compactor

### Compactor ring status

```
GET /compactor/ring
```

Displays a web page with the compactor hash ring status, including the state, healthy and last heartbeat time of each compactor.
