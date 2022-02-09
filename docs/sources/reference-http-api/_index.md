---
title: "Reference: HTTP API"
description: ""
weight: 10000
---

Cortex exposes an HTTP API for pushing and querying time series data, and operating the cluster itself.

For the sake of clarity, in this document we have grouped API endpoints by service, but keep in mind that they're exposed both when running Cortex in microservices and singly-binary mode:

- **Microservices**: each service exposes its own endpoints
- **Single-binary**: the Cortex process exposes all API endpoints for the services running internally

## Endpoints

| API                                                                                   | Service                 | Endpoint                                                             |
| ------------------------------------------------------------------------------------- | ----------------------- | -------------------------------------------------------------------- |
| [Index page](#index-page)                                                             | _All services_          | `GET /`                                                              |
| [Configuration](#configuration)                                                       | _All services_          | `GET /config`                                                        |
| [Runtime Configuration](#runtime-configuration)                                       | _All services_          | `GET /runtime_config`                                                |
| [Services status](#services-status)                                                   | _All services_          | `GET /services`                                                      |
| [Readiness probe](#readiness-probe)                                                   | _All services_          | `GET /ready`                                                         |
| [Metrics](#metrics)                                                                   | _All services_          | `GET /metrics`                                                       |
| [Pprof](#pprof)                                                                       | _All services_          | `GET /debug/pprof`                                                   |
| [Fgprof](#fgprof)                                                                     | _All services_          | `GET /debug/fgprof`                                                  |
| [Remote write](#remote-write)                                                         | Distributor             | `POST /api/v1/push`                                                  |
| [Tenants stats](#tenants-stats)                                                       | Distributor             | `GET /distributor/all_user_stats`                                    |
| [HA tracker status](#ha-tracker-status)                                               | Distributor             | `GET /distributor/ha_tracker`                                        |
| [Flush chunks / blocks](#flush-chunks--blocks)                                        | Ingester                | `GET,POST /ingester/flush`                                           |
| [Shutdown](#shutdown)                                                                 | Ingester                | `GET,POST /ingester/shutdown`                                        |
| [Ingesters ring status](#ingesters-ring-status)                                       | Ingester                | `GET /ingester/ring`                                                 |
| [Instant query](#instant-query)                                                       | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query`                     |
| [Range query](#range-query)                                                           | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query_range`               |
| [Exemplar query](#exemplar-query)                                                     | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query_exemplars`           |
| [Get series by label matchers](#get-series-by-label-matchers)                         | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/series`                    |
| [Get label names](#get-label-names)                                                   | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/labels`                    |
| [Get label values](#get-label-values)                                                 | Querier, Query-frontend | `GET <prometheus-http-prefix>/api/v1/label/{name}/values`            |
| [Get metric metadata](#get-metric-metadata)                                           | Querier, Query-frontend | `GET <prometheus-http-prefix>/api/v1/metadata`                       |
| [Remote read](#remote-read)                                                           | Querier, Query-frontend | `POST <prometheus-http-prefix>/api/v1/read`                          |
| [Label names cardinality](#label-names-cardinality)                                   | Querier, Query-frontend | `GET, POST <prometheus-http-prefix>/api/v1/cardinality/label_names`  |
| [Label values cardinality](#label-values-cardinality)                                 | Querier, Query-frontend | `GET, POST <prometheus-http-prefix>/api/v1/cardinality/label_values` |
| [Get tenant ingestion stats](#get-tenant-ingestion-stats)                             | Querier                 | `GET /api/v1/user_stats`                                             |
| [Ruler ring status](#ruler-ring-status)                                               | Ruler                   | `GET /ruler/ring`                                                    |
| [Ruler rules ](#ruler-rule-groups)                                                    | Ruler                   | `GET /ruler/rule_groups`                                             |
| [List rules](#list-rules)                                                             | Ruler                   | `GET <prometheus-http-prefix>/api/v1/rules`                          |
| [List alerts](#list-alerts)                                                           | Ruler                   | `GET <prometheus-http-prefix>/api/v1/alerts`                         |
| [List rule groups](#list-rule-groups)                                                 | Ruler                   | `GET /api/v1/rules`                                                  |
| [Get rule groups by namespace](#get-rule-groups-by-namespace)                         | Ruler                   | `GET /api/v1/rules/{namespace}`                                      |
| [Get rule group](#get-rule-group)                                                     | Ruler                   | `GET /api/v1/rules/{namespace}/{groupName}`                          |
| [Set rule group](#set-rule-group)                                                     | Ruler                   | `POST /api/v1/rules/{namespace}`                                     |
| [Delete rule group](#delete-rule-group)                                               | Ruler                   | `DELETE /api/v1/rules/{namespace}/{groupName}`                       |
| [Delete namespace](#delete-namespace)                                                 | Ruler                   | `DELETE /api/v1/rules/{namespace}`                                   |
| [Delete tenant configuration](#delete-tenant-configuration)                           | Ruler                   | `POST /ruler/delete_tenant_config`                                   |
| [Alertmanager status](#alertmanager-status)                                           | Alertmanager            | `GET /multitenant_alertmanager/status`                               |
| [Alertmanager configs](#alertmanager-configs)                                         | Alertmanager            | `GET /multitenant_alertmanager/configs`                              |
| [Alertmanager ring status](#alertmanager-ring-status)                                 | Alertmanager            | `GET /multitenant_alertmanager/ring`                                 |
| [Alertmanager UI](#alertmanager-ui)                                                   | Alertmanager            | `GET /<alertmanager-http-prefix>`                                    |
| [Alertmanager Delete Tenant Configuration](#alertmanager-delete-tenant-configuration) | Alertmanager            | `POST /multitenant_alertmanager/delete_tenant_config`                |
| [Get Alertmanager configuration](#get-alertmanager-configuration)                     | Alertmanager            | `GET /api/v1/alerts`                                                 |
| [Set Alertmanager configuration](#set-alertmanager-configuration)                     | Alertmanager            | `POST /api/v1/alerts`                                                |
| [Delete Alertmanager configuration](#delete-alertmanager-configuration)               | Alertmanager            | `DELETE /api/v1/alerts`                                              |
| [Tenant delete request](#tenant-delete-request)                                       | Purger                  | `POST /purger/delete_tenant`                                         |
| [Tenant delete status](#tenant-delete-status)                                         | Purger                  | `GET /purger/delete_tenant_status`                                   |
| [Store-gateway ring status](#store-gateway-ring-status)                               | Store-gateway           | `GET /store-gateway/ring`                                            |
| [Store-gateway tenants](#store-gateway-tenants)                                       | Store-gateway           | `GET /store-gateway/tenants`                                         |
| [Store-gateway tenant blocks](#store-gateway-tenant-blocks)                           | Store-gateway           | `GET /store-gateway/tenant/{tenant}/blocks`                          |
| [Compactor ring status](#compactor-ring-status)                                       | Compactor               | `GET /compactor/ring`                                                |

### Path prefixes

In this documentation you will find the usage of some placeholders for the path prefixes, whenever the prefix is configurable. The following table shows the supported prefixes.

| Prefix                       | Default         | CLI Flag                         | YAML Config                      |
| ---------------------------- | --------------- | -------------------------------- | -------------------------------- |
| `<legacy-http-prefix>`       | `/api/prom`     | `-http.prefix`                   | `http_prefix`                    |
| `<prometheus-http-prefix>`   | `/prometheus`   | `-http.prometheus-http-prefix`   | `api > prometheus_http_prefix`   |
| `<alertmanager-http-prefix>` | `/alertmanager` | `-http.alertmanager-http-prefix` | `api > alertmanager_http_prefix` |

### Authentication

When multi-tenancy is enabled, endpoints requiring authentication are expected to be called with the `X-Scope-OrgID` HTTP request header set to the tenant ID. Otherwise, when multi-tenancy is disabled, Cortex doesn't require any request to have the `X-Scope-OrgID` header.

Multi-tenancy can be enabled/disabled via the CLI flag `-auth.multitenancy-enabled` or its respective YAML config option.

_For more information, please refer to the dedicated [Authentication and Authorisation](../guides/authentication-and-authorisation.md) guide._

## All services

The following API endpoints are exposed by all services.

### Index page

```
GET /
```

Displays an index page with links to other web pages exposed by Cortex.

### Configuration

```
GET /config
```

Displays the configuration currently applied to Cortex (in YAML format), including default values and settings via CLI flags. Sensitive data is masked. Please be aware that the exported configuration **doesn't include the per-tenant overrides**.

#### Different modes

```
GET /config?mode=diff
```

Displays the configuration currently applied to Cortex (in YAML format) as before, but containing only the values that differ from the default values.

```
GET /config?mode=defaults
```

Displays the configuration using only the default values.

### Runtime Configuration

```
GET /runtime_config
```

Displays the runtime configuration currently applied to Cortex (in YAML format), including default values. Please be aware that the endpoint will be only available if Cortex is configured with the `-runtime-config.file` option.

#### Different modes

```
GET /runtime_config?mode=diff
```

Displays the runtime configuration currently applied to Cortex (in YAML format) as before, but containing only the values that differ from the default values.

### Services status

```
GET /services
```

Displays a web page with the status of internal Cortex services.

### Readiness probe

```
GET /ready
```

Returns 200 when Cortex is ready to serve traffic.

### Metrics

```
GET /metrics
```

Returns the metrics for the running Cortex service in the Prometheus exposition format.

### Pprof

```
GET /debug/pprof/heap
GET /debug/pprof/block
GET /debug/pprof/profile
GET /debug/pprof/trace
GET /debug/pprof/goroutine
GET /debug/pprof/mutex
```

Returns the runtime profiling data in the format expected by the pprof visualization tool. There are many things which can be profiled using this including heap, trace, goroutine, etc.

_For more information, please check out the official documentation of [pprof](https://golang.org/pkg/net/http/pprof/)._

### Fgprof

```
GET /debug/fgprof
```

Returns the sampling Go profiling data which allows you to analyze On-CPU as well as Off-CPU (e.g. I/O) time together.

_For more information, please check out the official documentation of [fgprof](https://github.com/felixge/fgprof)._

## Distributor

### Remote write

```
POST /api/v1/push

# Legacy
POST <legacy-http-prefix>/push
```

Entrypoint for the [Prometheus remote write](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

This API endpoint accepts an HTTP POST request with a body containing a request encoded with [Protocol Buffers](https://developers.google.com/protocol-buffers) and compressed with [Snappy](https://github.com/google/snappy). The definition of the protobuf message can be found in [`cortex.proto`](https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/cortex.proto#L12). The HTTP request should contain the header `X-Prometheus-Remote-Write-Version` set to `0.1.0`.

Also it is possible to skip the label name validation when sending series by doing two things: Enable API's flag `-api.skip-label-name-validation-header-enabled=true` and request must be sent with the header `X-Mimir-SkipLabelNameValidation: true`. This feature is useful to support the writes of downstream clients that have specific requirements.

_For more information, please check out Prometheus [Remote storage integrations](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations)._

_Requires [authentication](#authentication)._

### Distributor ring status

```
GET /distributor/ring
```

Displays a web page with the distributor hash ring status, including the state, healthy and last heartbeat time of each distributor.

### Tenants stats

```
GET /distributor/all_user_stats

# Legacy
GET /all_user_stats
```

Displays a web page with per-tenant statistics updated in realtime, including the total number of active series across all ingesters and the current ingestion rate (samples / sec).

### HA tracker status

```
GET /distributor/ha_tracker

# Legacy
GET /ha-tracker
```

Displays a web page with the current status of the HA tracker, including the elected replica for each Prometheus HA cluster.

## Ingester

### Flush chunks / blocks

```
GET,POST /ingester/flush

# Legacy
GET,POST /flush
```

Triggers a flush of the in-memory time series data (chunks or blocks) to the long-term storage. This endpoint triggers the flush also when `-ingester.flush-on-shutdown-with-wal-enabled` or `-blocks-storage.tsdb.flush-blocks-on-shutdown` are disabled.

This endpoint accepts `tenant` parameter to specify tenant whose blocks are compacted and shipped. This parameter may be specified multiple times to select more tenants. If no tenant is specified, all tenants are flushed.

Flush endpoint also accepts `wait=true` parameter, which makes the call synchronous – it will only return after flushing has finished. Note that returned status code does not reflect the result of flush operation.

### Shutdown

```
GET,POST /ingester/shutdown

# Legacy
GET,POST /shutdown
```

Flushes in-memory time series data from ingester to the long-term storage, and shuts down the ingester service. Notice that the other Cortex services are still running, and the operator (or any automation) is expected to terminate the process with a `SIGINT` / `SIGTERM` signal after the shutdown endpoint returns. In the meantime, `/ready` will not return 200. This endpoint will unregister the ingester from the ring even if `-ingester.unregister-on-shutdown` is disabled.

_This API endpoint is usually used by scale down automations._

### Ingesters ring status

```
GET /ingester/ring

# Legacy
GET /ring
```

Displays a web page with the ingesters hash ring status, including the state, healthy and last heartbeat time of each ingester.

## Querier / Query-frontend

The following endpoints are exposed both by the querier and query-frontend.

### Instant query

```
GET,POST <prometheus-http-prefix>/api/v1/query

# Legacy
GET,POST <legacy-http-prefix>/api/v1/query
```

Prometheus-compatible instant query endpoint.

_For more information, please check out the Prometheus [instant query](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries) documentation._

_Requires [authentication](#authentication)._

### Range query

```
GET,POST <prometheus-http-prefix>/api/v1/query_range

# Legacy
GET,POST <legacy-http-prefix>/api/v1/query_range
```

Prometheus-compatible range query endpoint. When the request is sent through the query-frontend, the query will be accelerated by query-frontend (results caching and execution parallelisation).

_For more information, please check out the Prometheus [range query](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries) documentation._

_Requires [authentication](#authentication)._

### Exemplar query

```
GET,POST <prometheus-http-prefix>/api/v1/query_exemplars

# Legacy
GET,POST <legacy-http-prefix>/api/v1/query_exemplars
```

Prometheus-compatible exemplar query endpoint.

_For more information, please check out the Prometheus [exemplar query](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-exemplars) documentation._

_Requires [authentication](#authentication)._

### Get series by label matchers

```
GET,POST <prometheus-http-prefix>/api/v1/series

# Legacy
GET,POST <legacy-http-prefix>/api/v1/series
```

_For more information, please check out the Prometheus [series endpoint](https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers) documentation._

_Requires [authentication](#authentication)._

### Get label names

```
GET,POST <prometheus-http-prefix>/api/v1/labels

# Legacy
GET,POST <legacy-http-prefix>/api/v1/labels
```

_For more information, please check out the Prometheus [get label names](https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names) documentation._

_Requires [authentication](#authentication)._

### Get label values

```
GET <prometheus-http-prefix>/api/v1/label/{name}/values

# Legacy
GET <legacy-http-prefix>/api/v1/label/{name}/values
```

_For more information, please check out the Prometheus [get label values](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values) documentation._

_Requires [authentication](#authentication)._

### Get metric metadata

```
GET <prometheus-http-prefix>/api/v1/metadata

# Legacy
GET <legacy-http-prefix>/api/v1/metadata
```

Prometheus-compatible metric metadata endpoint.

_For more information, please check out the Prometheus [metric metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metric-metadata) documentation._

_Requires [authentication](#authentication)._

### Remote read

```
POST <prometheus-http-prefix>/api/v1/read

# Legacy
POST <legacy-http-prefix>/api/v1/read
```

Prometheus-compatible [remote read](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read) endpoint.

_For more information, please check out Prometheus [Remote storage integrations](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations)._

_Requires [authentication](#authentication)._

### Label names cardinality

```
GET,POST <prometheus-http-prefix>/api/v1/cardinality/label_names

# Legacy
GET,POST <legacy-http-prefix>/api/v1/cardinality/label_names
```

Returns realtime label names cardinality across all ingesters, for the authenticated tenant, in `JSON` format.
It counts distinct label values per label name.

As far as this endpoint generates cardinality report using only values from currently opened TSDBs in ingesters, two subsequent calls may return completely different results, if ingester did a block
cutting between the calls.

The items in the field `cardinality` are sorted by `label_values_count` in DESC order and by `label_name` in ASC order.

The count of items is limited by `limit` request param.

_This endpoint is disabled by default and can be enabled via the `-querier.cardinality-analysis-enabled` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

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

# Legacy
GET,POST <legacy-http-prefix>/api/v1/cardinality/label_values
```

Returns realtime label values cardinality associated to request param `label_names[]` across all ingesters, for the authenticated tenant, in `JSON` format.
It returns the series count per label value associated to request param `label_names[]`.

As far as this endpoint generates cardinality report using only values from currently opened TSDBs in ingesters, two subsequent calls may return completely different results, if ingester did a block
cutting between the calls.

The items in the field `labels` are sorted by `series_count` in DESC order and by `label_name` in ASC order.
The items in the field `cardinality` are sorted by `series_count` in DESC order and by `label_value` in ASC order.

The count of `cardinality` items is limited by request param `limit`.

_This endpoint is disabled by default and can be enabled via the `-querier.cardinality-analysis-enabled` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

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

# Legacy
GET <legacy-http-prefix>/user_stats
```

Returns realtime ingestion rate, for the authenticated tenant, in `JSON` format.

_Requires [authentication](#authentication)._

## Ruler

The ruler API endpoints require to configure a backend object storage to store the recording rules and alerts. The ruler API uses the concept of a "namespace" when creating rule groups. This is a stand in for the name of the rule file in Prometheus and rule groups must be named uniquely within a namespace.

### Ruler ring status

```
GET /ruler/ring

# Legacy
GET /ruler_ring
```

Displays a web page with the ruler hash ring status, including the state, healthy and last heartbeat time of each ruler.

### Ruler rules

```
GET /ruler/rule_groups
```

List all tenant rules. This endpoint is not part of ruler-API and is always available regardless of whether ruler-API is enabled or not. It should not be exposed to end users. This endpoint returns a YAML dictionary with all the rule groups for each tenant and `200` status code on success.

### List rules

```
GET <prometheus-http-prefix>/api/v1/rules

# Legacy
GET <legacy-http-prefix>/api/v1/rules
```

Prometheus-compatible rules endpoint to list alerting and recording rules that are currently loaded.

_For more information, please check out the Prometheus [rules](https://prometheus.io/docs/prometheus/latest/querying/api/#rules) documentation._

_This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### List alerts

```
GET <prometheus-http-prefix>/api/v1/alerts

# Legacy
GET <legacy-http-prefix>/api/v1/alerts
```

Prometheus-compatible rules endpoint to list of all active alerts.

_For more information, please check out the Prometheus [alerts](https://prometheus.io/docs/prometheus/latest/querying/api/#alerts) documentation._

_This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### List rule groups

```
GET /api/v1/rules

# Legacy
GET <legacy-http-prefix>/rules
```

List all rules configured for the authenticated tenant. This endpoint returns a YAML dictionary with all the rule groups for each namespace and `200` status code on success.

_This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

#### Example response

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
GET /api/v1/rules/{namespace}

# Legacy
GET <legacy-http-prefix>/rules/{namespace}
```

Returns the rule groups defined for a given namespace.

_This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

#### Example response

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
GET /api/v1/rules/{namespace}/{groupName}

# Legacy
GET <legacy-http-prefix>/rules/{namespace}/{groupName}
```

Returns the rule group matching the request namespace and group name.

_This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### Set rule group

```
POST /api/v1/rules/{namespace}

# Legacy
POST <legacy-http-prefix>/rules/{namespace}
```

Creates or updates a rule group. This endpoint expects a request with `Content-Type: application/yaml` header and the
rules **YAML** definition in the request body, and returns `202` on success.

_This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

#### Federated rule groups

A federated rule groups is a rule group with a non-empty `source_tenants`.

The `source_tenants` field allows aggregating data from multiple tenants while evaluating a rule group. The expressions
of each rule in the group will be evaluated against the data of all tenants in `source_tenants`. If `source_tenants` is
empty or omitted, then the tenant under which the group is created will be treated as the `source_tenant`.

Federated rule groups are skipped during evaluation by default. This feature depends on
the [cross-tenant query federation](../proposals/cross-tenant-query-federation.md) feature. To enable federated rules
set `-ruler.tenant-federation.enabled=true` and `-tenant-federation.enabled=true` CLI flags (or their respective YAML
config options).

During evaluation query limits applied to single tenants are also applied to each query in the rule group. For example,
if `tenant-a` has a federated rule group with `source_tenants: [tenant-b, tenant-c]`, then query limits for `tenant-b`
and `tenant-c` will be applied. If any of these limits is exceeded, the whole evaluation will fail. No partial results
will be saved. The same "no partial results" guarantee applies to queries failing for other reasons (e.g. ingester
unavailability).

The time series used during evaluation of federated rules will have the `__tenant_id__` label, similar to how it is
present on series returned with [cross-tenant query federation](../proposals/cross-tenant-query-federation.md).

**Considerations:** Federated rule groups allow data from multiple source tenants to be written into a single
destination tenant. This makes the existing separation of tenants' data less clear. For example, `tenant-a` has a
federated rule group that aggregates over `tenant-b`'s data (e.g. `sum(metric_b)`) and writes the result back
into `tenant-a`'s storage (e.g. as metric `sum:metric_b`). Now part of `tenant-b`'s data is copied to `tenant-a` (albeit
aggregated). Have this in mind when configuring the access control layer in front of mimir and when enabling federated
rules via `-ruler.tenant-federation.enabled`.

#### Example request

Request headers:

- `Content-Type: application/yaml`

Request body:

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
DELETE /api/v1/rules/{namespace}/{groupName}

# Legacy
DELETE <legacy-http-prefix>/rules/{namespace}/{groupName}
```

Deletes a rule group by namespace and group name. This endpoints returns `202` on success.

_This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### Delete namespace

```
DELETE /api/v1/rules/{namespace}

# Legacy
DELETE <legacy-http-prefix>/rules/{namespace}
```

Deletes all the rule groups in a namespace (including the namespace itself). This endpoint returns `202` on success.

_This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### Delete tenant configuration

```
POST /ruler/delete_tenant_config
```

This deletes all rule groups for tenant, and returns `200` on success. Calling endpoint when no rule groups exist for user returns `200`. Authentication is only to identify the tenant.

This is intended as internal API, and not to be exposed to users. This endpoint is enabled regardless of whether `-ruler.enable-api` is enabled or not.

_Requires [authentication](#authentication)._

## Alertmanager

### Alertmanager status

```
GET /multitenant_alertmanager/status

# Legacy (microservices mode only)
GET /status
```

Displays a web page with the current status of the Alertmanager, including the Alertmanager cluster members.

### Alertmanager configs

```
GET /multitenant_alertmanager/configs
```

List all Alertmanager configurations. This endpoint is not part of alertmanager-API and is always available regardless of whether alertmanager-API is enabled or not. It should not be exposed to end users. This endpoint returns a YAML dictionary with all the Alertmanager configurations and `200` status code on success.

### Alertmanager ring status

```
GET /multitenant_alertmanager/ring
```

Displays a web page with the Alertmanager hash ring status, including the state, healthy and last heartbeat time of each Alertmanager instance.

### Alertmanager UI

```
GET /<alertmanager-http-prefix>

# Legacy (microservices mode only)
GET /<legacy-http-prefix>
```

Displays the Alertmanager UI.

_Requires [authentication](#authentication)._

### Alertmanager Delete Tenant Configuration

```
POST /multitenant_alertmanager/delete_tenant_config
```

This endpoint deletes configuration for a tenant identified by `X-Scope-OrgID` header.
It is internal, available even if Alertmanager API is disabled.
The endpoint returns a status code of `200` if the user's configuration has been deleted, or it didn't exist in the first place.

_Requires [authentication](#authentication)._

### Get Alertmanager configuration

```
GET /api/v1/alerts
```

Get the current Alertmanager configuration for the authenticated tenant, reading it from the configured object storage.

This endpoint doesn't accept any URL query parameter and returns `200` on success.

_This endpoint can disabled enabled via the `-alertmanager.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### Set Alertmanager configuration

```
POST /api/v1/alerts
```

Stores or updates the Alertmanager configuration for the authenticated tenant. The Alertmanager configuration is stored in the configured backend object storage.

This endpoint expects the Alertmanager **YAML** configuration in the request body and returns `201` on success.

_This endpoint can disabled enabled via the `-alertmanager.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

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

_This endpoint can be disabled via the `-alertmanager.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

## Purger

The Purger service provides APIs for requesting tenant deletion.

### Tenant Delete Request

```
POST /purger/delete_tenant
```

Request deletion of ALL tenant data. Experimental.

_Requires [authentication](#authentication)._

### Tenant Delete Status

```
GET /purger/delete_tenant_status
```

Returns status of tenant deletion. Output format to be defined. Experimental.

_Requires [authentication](#authentication)._

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
