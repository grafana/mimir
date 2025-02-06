---
aliases:
  - ../operators-guide/reference-http-api/
description:
  Use the HTTP API to write and query time-series data, and to operate
  a Grafana Mimir cluster.
keywords:
  - Mimir API
  - Mimir endpoints
  - Mimir communication
  - Mimir querying
menuTitle: HTTP API
title: Grafana Mimir HTTP API
weight: 120
---

# Grafana Mimir HTTP API

Grafana Mimir exposes an HTTP API that you can use to write and query time series data, and operate the cluster.

This document groups API endpoints by service. Note that the API endpoints are exposed when you run Grafana Mimir in microservices mode, monolithic mode, and read-write mode:

- **Microservices mode**: Each service exposes its own endpoints.
- **Monolithic mode**: The Grafana Mimir instance exposes all API endpoints.
- **Read-write mode**: The component services are exposed on the endpoint that they are contained within. Either Mimir read, Mimir write, or Mimir backend. Refer to [Deployment modes]({{< relref "../architecture/deployment-modes" >}}) for the grouping of components.

## Endpoints

{{% responsive-table %}}
| API | Service | Endpoint |
| ------------------------------------------------------------------------------------- | ------------------------------ | ------------------------------------------------------------------------- |
| [Index page](#index-page) | _All services_ | `GET /` |
| [Configuration](#configuration) | _All services_ | `GET /config` |
| [Status Configuration](#status-configuration) | _All services_ | `GET /api/v1/status/config` |
| [Status Flags](#status-flags) | _All services_ | `GET /api/v1/status/flags` |
| [Runtime Configuration](#runtime-configuration) | _All services_ | `GET /runtime_config` |
| [Services' status](#services-status) | _All services_ | `GET /services` |
| [Readiness probe](#readiness-probe) | _All services_ | `GET /ready` |
| [Metrics](#metrics) | _All services_ | `GET /metrics` |
| [Pprof](#pprof) | _All services_ | `GET /debug/pprof` |
| [Fgprof](#fgprof) | _All services_ | `GET /debug/fgprof` |
| [Build information](#build-information) | _All services_ | `GET /api/v1/status/buildinfo` |
| [Memberlist cluster](#memberlist-cluster) | _All services_ | `GET /memberlist` |
| [Get tenant limits](#get-tenant-limits) | _All services_ | `GET /api/v1/user_limits` |
| [Remote write](#remote-write) | Distributor | `POST /api/v1/push` |
| [OTLP](#otlp) | Distributor | `POST /otlp/v1/metrics` |
| [Tenants stats](#tenants-stats) | Distributor | `GET /distributor/all_user_stats` |
| [HA tracker status](#ha-tracker-status) | Distributor | `GET /distributor/ha_tracker` |
| [Flush chunks / blocks](#flush-chunks--blocks) | Ingester | `GET,POST /ingester/flush` |
| [Prepare for Shutdown](#prepare-for-shutdown) | Ingester | `GET,POST,DELETE /ingester/prepare-shutdown` |
| [Shutdown](#shutdown) | Ingester | `POST /ingester/shutdown` |
| [Prepare Partition Downscale](#prepare-partition-downscale) | Ingester | `GET,POST,DELETE /ingester/prepare-partition-downscale` |
| [Prepare Instance Ring Downscale](#prepare-instance-ring-downscale) | Ingester | `GET,POST,DELETE /ingester/prepare-instance-ring-downscale` |
| [Ingesters ring status](#ingesters-ring-status) | Distributor,Ingester | `GET /ingester/ring` |
| [Ingester tenants](#ingester-tenants) | Ingester | `GET /ingester/tenants` |
| [Ingester tenant TSDB](#ingester-tenant-tsdb) | Ingester | `GET /ingester/tsdb/{tenant}` |
| [Instant query](#instant-query) | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query` |
| [Range query](#range-query) | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query_range` |
| [Exemplar query](#exemplar-query) | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query_exemplars` |
| [Get series by label matchers](#get-series-by-label-matchers) | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/series` |
| [Get active series by selector](#get-active-series-by-selector) | Query-frontend | `GET, POST <prometheus-http-prefix>/api/v1/cardinality/active_series` |
| [Get label names](#get-label-names) | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/labels` |
| [Get label values](#get-label-values) | Querier, Query-frontend | `GET <prometheus-http-prefix>/api/v1/label/{name}/values` |
| [Get metric metadata](#get-metric-metadata) | Querier, Query-frontend | `GET <prometheus-http-prefix>/api/v1/metadata` |
| [Remote read](#remote-read) | Querier, Query-frontend | `POST <prometheus-http-prefix>/api/v1/read` |
| [Label names cardinality](#label-names-cardinality) | Querier, Query-frontend | `GET, POST <prometheus-http-prefix>/api/v1/cardinality/label_names` |
| [Label values cardinality](#label-values-cardinality) | Querier, Query-frontend | `GET, POST <prometheus-http-prefix>/api/v1/cardinality/label_values` |
| [Build information](#build-information) | Querier, Query-frontend, Ruler | `GET <prometheus-http-prefix>/api/v1/status/buildinfo` |
| [Format query](#format-query) | Querier, Query-frontend | `GET, POST <prometheus-http-prefix>/api/v1/format_query` |
| [Get tenant ingestion stats](#get-tenant-ingestion-stats) | Querier | `GET /api/v1/user_stats` |
| [Query-scheduler ring status](#query-scheduler-ring-status) | Query-scheduler | `GET /query-scheduler/ring` |
| [Ruler ring status](#ruler-ring-status) | Ruler | `GET /ruler/ring` |
| [Ruler rules ](#ruler-rules) | Ruler | `GET /ruler/rule_groups` |
| [List Prometheus rules](#list-prometheus-rules) | Ruler | `GET <prometheus-http-prefix>/api/v1/rules` |
| [List Prometheus alerts](#list-prometheus-alerts) | Ruler | `GET <prometheus-http-prefix>/api/v1/alerts` |
| [List rule groups](#list-rule-groups) | Ruler | `GET <prometheus-http-prefix>/config/v1/rules` |
| [Get rule groups by namespace](#get-rule-groups-by-namespace) | Ruler | `GET <prometheus-http-prefix>/config/v1/rules/{namespace}` |
| [Get rule group](#get-rule-group) | Ruler | `GET <prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}` |
| [Set rule group](#set-rule-group) | Ruler | `POST <prometheus-http-prefix>/config/v1/rules/{namespace}` |
| [Delete rule group](#delete-rule-group) | Ruler | `DELETE <prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}` |
| [Delete namespace](#delete-namespace) | Ruler | `DELETE <prometheus-http-prefix>/config/v1/rules/{namespace}` |
| [Delete tenant configuration](#delete-tenant-configuration) | Ruler | `POST /ruler/delete_tenant_config` |
| [Alertmanager status](#alertmanager-status) | Alertmanager | `GET /multitenant_alertmanager/status` |
| [Alertmanager configs](#alertmanager-configs) | Alertmanager | `GET /multitenant_alertmanager/configs` |
| [Alertmanager ring status](#alertmanager-ring-status) | Alertmanager | `GET /multitenant_alertmanager/ring` |
| [Alertmanager UI](#alertmanager-ui) | Alertmanager | `GET <alertmanager-http-prefix>` |
| [Build Information](#build-information) | Alertmanager | `GET <alertmanager-http-prefix>/api/v1/status/buildinfo` |
| [Alertmanager Delete Tenant Configuration](#alertmanager-delete-tenant-configuration) | Alertmanager | `POST /multitenant_alertmanager/delete_tenant_config` |
| [Get Alertmanager configuration](#get-alertmanager-configuration) | Alertmanager | `GET /api/v1/alerts` |
| [Set Alertmanager configuration](#set-alertmanager-configuration) | Alertmanager | `POST /api/v1/alerts` |
| [Delete Alertmanager configuration](#delete-alertmanager-configuration) | Alertmanager | `DELETE /api/v1/alerts` |
| [Store-gateway ring status](#store-gateway-ring-status) | Store-gateway | `GET /store-gateway/ring` |
| [Store-gateway tenants](#store-gateway-tenants) | Store-gateway | `GET /store-gateway/tenants` |
| [Store-gateway tenant blocks](#store-gateway-tenant-blocks) | Store-gateway | `GET /store-gateway/tenant/{tenant}/blocks` |
| [Prepare for Shutdown](#prepare-for-shutdown) | Store-gateway | `GET,POST,DELETE /store-gateway/prepare-shutdown` |
| [Compactor ring status](#compactor-ring-status) | Compactor | `GET /compactor/ring` |
| [Start block upload](#start-block-upload) | Compactor | `POST /api/v1/upload/block/{block}/start` |
| [Upload block file](#upload-block-file) | Compactor | `POST /api/v1/upload/block/{block}/files?path={path}` |
| [Complete block upload](#complete-block-upload) | Compactor | `POST /api/v1/upload/block/{block}/finish` |
| [Check block upload](#check-block-upload) | Compactor | `GET /api/v1/upload/block/{block}/check` |
| [Tenant delete request](#tenant-delete-request) | Compactor | `POST /compactor/delete_tenant` |
| [Tenant delete status](#tenant-delete-status) | Compactor | `GET /compactor/delete_tenant_status` |
| [Compactor tenants](#compactor-tenants) | Compactor | `GET /compactor/tenants` |
| [Compactor tenant planned jobs](#compactor-tenant-planned-jobs) | Compactor | `GET /compactor/tenant/{tenant}/planned_jobs` |
| [Overrides-exporter ring status](#overrides-exporter-ring-status) | Overrides-exporter | `GET /overrides-exporter/ring` |
{{% /responsive-table %}}

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

For more information about authentication and authorization, refer to [Authentication and Authorization]({{< relref "../../manage/secure/authentication-and-authorization" >}}).

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

{{< admonition type="note" >}}
The exported configuration doesn't include the per-tenant overrides.
{{< /admonition >}}

#### Different modes

```
GET /config?mode=diff
```

This endpoint displays the differences between the Grafana Mimir default configuration and the current configuration.

```
GET /config?mode=defaults
```

This endpoint displays the default configuration values.

### Status Configuration

```
GET /api/v1/status/config
```

This endpoint displays empty configuration settings, it exists only to be compatible with the Prometheus `/api/v1/status/config` API.

### Status Flags

```
GET /api/v1/status/flags
```

This endpoint displays empty configuration flags, it exists only to be compatible with the Prometheus `/api/v1/status/flags` API.

### Runtime Configuration

```
GET /runtime_config
```

This endpoint displays the [runtime configuration]({{< relref "../../configure/about-runtime-configuration" >}}) currently applied to Grafana Mimir, in YAML format, including default values.
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

### Format query

```
GET <prometheus-http-prefix>/api/v1/format_query?query={query}
POST <prometheus-http-prefix>/api/v1/format_query
```

Formats the PromQL query.

This endpoint is compatible with the Prometheus format query endpoint.

For more information about formatting queries, refer to [Prometheus' documentation](https://prometheus.io/docs/prometheus/latest/querying/api/#formatting-query-expressions).

### Memberlist cluster

```
GET /memberlist
```

This admin page shows information about Memberlist cluster (list of nodes and their health) and KV store (keys and values in the KV store).

If memberlist message history is enabled, this page also shows all received and sent messages stored in the buffers.
This can be useful for troubleshooting memberlist cluster.
To enable message history buffers use `-memberlist.message-history-buffer-bytes` CLI flag or the corresponding YAML configuration parameter.

### Get tenant limits

```
GET /api/v1/user_limits
```

Returns realtime limits for the authenticated tenant, in `JSON` format.
This API is experimental.

Requires [authentication](#authentication).

The endpoint is only available if Grafana Mimir is configured with the `-runtime-config.file` option.

## Distributor

The following endpoints relate to the [distributor]({{< relref "../architecture/components/distributor" >}}).

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

### OTLP

```
POST /otlp/v1/metrics
```

{{% admonition type="note" %}}
To send OTLP data to Grafana Cloud, refer to [Send data using OpenTelemetry Protocol (OTLP)](https://grafana.com/docs/grafana-cloud/send-data/otlp/send-data-otlp/).
{{% /admonition %}}

Entrypoint for the [OTLP HTTP](https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md).

This endpoint accepts an HTTP POST request with a body that contains a request encoded with [Protocol Buffers](https://developers.google.com/protocol-buffers) and optionally compressed with [GZIP](https://www.gnu.org/software/gzip/).
You can find the definition of the protobuf message in [metrics.proto](https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto).

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

{{< admonition type="note" >}}
This endpoint requires all ingesters to be `ACTIVE` in the ring for a successful response.
{{< /admonition >}}

### HA tracker status

```
GET /distributor/ha_tracker
```

This endpoint displays a web page with the current status of the HA tracker, including the elected replica for each Prometheus HA cluster.

## Ingester

The following endpoints relate to the [ingester]({{< relref "../architecture/components/ingester" >}}).

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

{{< admonition type="note" >}}
The returned status code doesn't reflect the result of flush operation.
{{< /admonition >}}

### Prepare for Shutdown

```
GET,POST,DELETE /ingester/prepare-shutdown
```

This endpoint inspects or changes in-memory ingester configuration to prepare for permanently stopping an ingester
instance but does not actually stop any part of the ingester.

After a `POST` to the `prepare-shutdown` endpoint returns, when the ingester process is stopped with `SIGINT` / `SIGTERM`,
the ingester will be unregistered from the ring and in-memory time series data will be flushed to long-term storage.
This endpoint causes the ingester to be unregistered from the ring when stopped even if you disable
`-ingester.ring.unregister-on-shutdown`.

A `GET` to the `prepare-shutdown` endpoint returns the status of this configuration, either `set` or `unset`.

A `DELETE` to the `prepare-shutdown` endpoint reverts the configuration of the ingester to its previous state
(with respect to unregistering on shutdown and flushing of in-memory time series data to long-term storage).

This API endpoint is usually used by Kubernetes-specific scale down automations such as the
[rollout-operator](https://github.com/grafana/rollout-operator).

### Shutdown

```
POST /ingester/shutdown
```

This endpoint flushes in-memory time series data from ingesters to the long-term storage, and then shuts down the ingester service.
After the shutdown endpoint returns, the operator or any automation that's used terminates the process with a `SIGINT` / `SIGTERM` signal.
During this time, `/ready` does not return 200.
This endpoint unregisters the ingester from the ring even if you disable `-ingester.ring.unregister-on-shutdown`.

This API endpoint is usually used by scale down automations.

### Prepare partition downscale

```
GET,POST,DELETE /ingester/prepare-partition-downscale
```

This endpoint prepares the ingester's partition for downscaling by setting it to the `INACTIVE` state.

A `GET` call to this endpoint returns a timestamp of when the partition was switched to the `INACTIVE` state, or 0, if the partition is not in the `INACTIVE` state.

A `POST` call switches this ingester's partition to the `INACTIVE` state, if it isn't `INACTIVE` already, and returns the timestamp of when the switch to the `INACTIVE` state occurred.

A `DELETE` call sets the partition back from the `INACTIVE` to the `ACTIVE` state.

If the ingester is not configured to use ingest-storage, any call to this endpoint fails.

This API endpoint is usually used by scale down automation, e.g. rollout-operator.

### Prepare instance ring downscale

```
GET,POST,DELETE /ingester/prepare-instance-ring-downscale
```

This endpoint prepares the ingester for downscaling by setting it to read-only mode.

A `GET` call to this endpoint returns a timestamp of when the ingester was switched to read-only mode, or 0, if the ingester is not in read-only mode.

A `POST` call switches this ingester's partition to read-only mode, if it isn't read-only already, and returns the timestamp of when the switch to read-only mode occurred.

A `DELETE` call sets the ingester back to read-write mode.

If the ingester is configured to use ingest-storage, any call to this endpoint fails.

This API endpoint is usually used by scale down automation, e.g. rollout-operator.

### Prepare for unregister

```
GET,PUT,DELETE /ingester/unregister-on-shutdown
```

This endpoint controls whether an ingester should unregister from the ring on its next termination, that is, the next time it receives a `SIGINT` or `SIGTERM` signal.
Via this endpoint, Mimir operators can dynamically control an ingester's `-ingester.ring.unregister-on-shutdown` state without having to restart the ingester.

A `PUT` sets the ingester's unregister state. When invoked with the `PUT` method, the endpoint takes a request body:

```
{"unregister": true}
```

A `GET` returns the ingester's current unregister state.

A `DELETE` resets the ingester's unregister state to the value that was passed via the `-ingester.ring.unregister-on-shutdown`
configuration option.

Regardless of the HTTP method used, the endpoint always returns a response body with the ingester's current unregister state:

```
{"unregister": true}
```

### TSDB Metrics

```
GET /ingester/tsdb_metrics
```

This endpoint returns low-level metrics exposed by per-tenant TSDB. This can be useful for troubleshooting.
Difference from `/metrics` is that metrics returned in `/metrics` are aggregated across all open TSDBs, while this
endpoint returns TSDB metrics only for specific tenant.

Requires [authentication](#authentication), authenticated tenant is one whose TSDB metrics are returned.

### Ingesters ring status

```
GET /ingester/ring
```

This endpoint displays a web page with the ingesters hash ring status, including the state, health, and last heartbeat time of each ingester.

### Ingester tenants

```
GET /ingester/tenants
```

Displays a web page with the list of tenants with open TSDB on given ingester.

### Ingester tenant TSDB

```
GET /ingester/tsdb/{tenant}
```

Displays a web page with details about tenant's open TSDB on given ingester.

## Querier / Query-frontend

The following endpoints are exposed both by the [querier]({{< relref "../architecture/components/querier" >}}) and [query-frontend]({{< relref "../architecture/components/query-frontend" >}}).

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

### Get active series by selector

```
GET,POST <prometheus-http-prefix>/api/v1/cardinality/active_series
```

Returns the label sets of all active series matching a PromQL selector.

This endpoint is similar to the [series endpoint](#get-series-by-label-matchers) but operates on the set of series considered _active_ at the time of query processing.
A series is considered active if any data has been written for it within the period specified by `-ingester.active-series-metrics-idle-timeout`.

This endpoint is disabled by default; you can enable it via the `-querier.cardinality-analysis-enabled` CLI flag (or its respective YAML configuration option).

Requires [authentication](#authentication).

#### Query parameters

- **selector** - _mandatory_ - PromQL selector used to filter the result set.

#### Headers

- `Sharding-Control` - _optional_ - Integer value specifying how many shards to use for request execution.

#### Response format

The response format is a subset of the [series endpoint](#get-series-by-label-matchers) format including only the `data` field.
The following shows an example request/response pair for this endpoint. Each item in the `data` array corresponds to a matched series.

```shell
$ curl 'http://localhost:9090/api/v1/cardinality/active_series' \
    --header 'Sharding-Control: 4' \ # optional
    --data-urlencode 'selector=up'
```

```shell
{
   "data" : [
      {
         "__name__" : "up",
         "job" : "prometheus",
         "instance" : "localhost:9090"
      },
      {
         "__name__" : "up",
         "job" : "node",
         "instance" : "localhost:9091"
      },
      {
         "__name__" : "process_start_time_seconds",
         "job" : "prometheus",
         "instance" : "localhost:9090"
      }
   ]
}
```

#### Caching

Responses for the active series endpoint are never cached.

### Get label names

```
GET,POST <prometheus-http-prefix>/api/v1/labels
```

For more information, refer to Prometheus [get label names](https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names).

Requires [authentication](#authentication).

#### Caching

The query-frontend can return a stale response fetched from the query results cache if `-query-frontend.cache-results` is enabled and `-query-frontend.results-cache-ttl-for-labels-query` set to a value greater than `0`.

### Get label values

```
GET <prometheus-http-prefix>/api/v1/label/{name}/values
```

For more information, refer to Prometheus [get label values](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values).

Requires [authentication](#authentication).

#### Caching

The query-frontend can return a stale response fetched from the query results cache if `-query-frontend.cache-results` is enabled and `-query-frontend.results-cache-ttl-for-labels-query` set to a value greater than `0`.

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

Returns label names cardinality across all ingesters, for the authenticated tenant, in `JSON` format.
It counts distinct label values per label name.

The items in the field `cardinality` are sorted by `label_values_count` in DESC order and by `label_name` in ASC order.
The count of items is limited by `limit` request param.

This endpoint is disabled by default and can be enabled via the `-querier.cardinality-analysis-enabled` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

#### Count series by `inmemory` or `active`

Two methods of counting are available: `inmemory` and `active`. To choose one, use the `count_method` parameter.

The `inmemory` method counts the labels in currently opened TSDBs in Mimir's ingesters.
Two subsequent calls might return completely different results if an ingester cut a block between calls.
This method of counting is most useful for understanding ingester memory usage.

The `active` method also counts labels in currently opened TSDBs in Mimir's ingesters, but filters out values that have not received a sample within a configurable duration of time.
To configure this duration, use the `-ingester.active-series-metrics-idle-timeout` parameter.
This method of counting is most useful for understanding what label values are represented in the samples ingested by Mimir in the last `-ingester.active-series-metrics-idle-timeout`.
Two subsequent calls will likely return similar results, because this window of time is not related to the block cutting on ingesters.
Values will change only as a result of changes in the data ingested by Mimir.

#### Caching

The query-frontend can return a stale response fetched from the query results cache if `-query-frontend.cache-results` is enabled and `-query-frontend.results-cache-ttl-for-cardinality-query` set to a value greater than `0`.

#### Request params

- **selector** - _optional_ - specifies PromQL selector that will be used to filter series that must be analyzed.
- **count_method** - _optional_ - specifies which series counting method will be used. (default="inmemory", available options=["inmemory", "active"])
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

Returns label values cardinality associated to request param `label_names[]` across all ingesters, for the authenticated tenant, in `JSON` format.
It returns the series count per label value associated to request param `label_names[]`.

The items in the field `labels` are sorted by `series_count` in descending order and by `label_name` in ascending order.
The items in the field `cardinality` are sorted by `series_count` in DESC order and by `label_value` in ASC order.
The count of `cardinality` items is limited by request parameter `limit`.

This endpoint is disabled by default; you can enable it via the `-querier.cardinality-analysis-enabled` CLI flag (or its respective YAML configuration option).

Requires [authentication](#authentication).

#### Count series by `inmemory` or `active`

Two methods of counting are available: `inmemory` and `active`. To choose one, use the `count_method` parameter.

The `inmemory` method counts the number of series in currently opened TSDBs in Mimir's ingesters.
Two subsequent calls might return completely different results if an ingester cut a block between calls.
This method of counting is most useful for understanding ingester memory usage.

The `active` method also counts series in currently opened TSDBs in Mimir's ingesters, but filters out series that have not received a sample within a configurable duration of time.
To configure this duration, use the `-ingester.active-series-metrics-idle-timeout` parameter.
This method of counting is most useful for understanding what label values are represented in the samples ingested by Mimir in the last `-ingester.active-series-metrics-idle-timeout`.
Two subsequent calls will likely return similar results, because this window of time is not related to the block cutting on ingesters.
Values will change only as a result of changes in the data ingested by Mimir.

#### Caching

The query-frontend can return a stale response fetched from the query results cache if `-query-frontend.cache-results` is enabled and `-query-frontend.results-cache-ttl-for-cardinality-query` set to a value greater than `0`.

#### Request params

- **label_names[]** - _required_ - specifies labels for which cardinality must be provided.
- **selector** - _optional_ - specifies PromQL selector that will be used to filter series that must be analyzed.
- **count_method** - _optional_ - specifies which series counting method will be used. (default="inmemory", available options=["inmemory", "active"])
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

## Query-scheduler

### Query-scheduler ring status

```
GET /query-scheduler/ring
```

Displays a web page with the query-scheduler hash ring status, including the state, healthy and last heartbeat time of each query-scheduler.
The query-scheduler ring is available only when `-query-scheduler.service-discovery-mode` is set to `ring`.

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
GET <prometheus-http-prefix>/api/v1/rules?type={alert|record}&file={}&rule_group={}&rule_name={}&exclude_alerts={true|false}
```

Prometheus-compatible rules endpoint to list alerting and recording rules that are currently loaded.

The `type` parameter is optional. If set, only the specified type of rule is returned.

The `file`, `rule_group` and `rule_name` parameters are optional, and can accept multiple values. If set, the response content is filtered accordingly. The parameters can also be provided as `file[]`, `rule_group[]` and `rule_name[]` - if both are provided e.g `file` and `file[]` , `file[]` will take precdent.

The `exclude_alerts` parameter is optional. If set, it only returns rules and excludes active alerts.

The `group_limit` and `group_next_token` parameters are optional. If `group_limit` is set, it will limit the number of rule groups returned in a single response. If the total number of rule groups exceeds this value, the response will contain a `groupNextToken`.
This can be passed into subsequent requests via `group_next_token` to paginate over the remaining groups. The final response will not contain a token.
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
```

List all rules configured for the authenticated tenant. This endpoint returns a YAML dictionary with all the rule groups for each namespace and `200` status code on success.

This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

{{< admonition type="note" >}}
To list all rule groups from Mimir, use [`mimirtool rules list` command]({{< relref "../../manage/tools/mimirtool#list-rules" >}}).
{{< /admonition >}}

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
```

Returns the rule groups defined for a given namespace.
Escape the `{namespace}` path segment using percent-encoding, as defined by
[RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986). For example, escape `/` to `%2F`.

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
```

Returns the rule group matching the request namespace and group name.
Escape the `{namespace}` and `{groupName}` path segments using percent-encoding, as defined by
[RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986). For example, escape `/` to `%2F`.

This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

{{< admonition type="note" >}}
To retrieve a single rule group from Mimir, use [`mimirtool rules get` command]({{< relref "../../manage/tools/mimirtool#get-rule-group" >}}) .
{{< /admonition >}}

### Set rule group

```
POST /<prometheus-http-prefix>/config/v1/rules/{namespace}
```

Creates or updates a rule group.
This endpoint expects a request with `Content-Type: application/yaml` header and the rules group **YAML** definition in the request body, and returns `202` on success.
The request body must contain the definition of one and only one rule group.
Escape the `{namespace}` path segment using percent-encoding, as defined by [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).
For example, escape `/` to `%2F`.

This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

{{< admonition type="note" >}}
To load one or more rule groups into Mimir, use [`mimirtool rules load` command]({{< relref "../../manage/tools/mimirtool#load-rule-group" >}}) .
{{< /admonition >}}

{{< admonition type="note" >}}
When using `curl` to send the request body from a file, ensure that you use the `--data-binary` flag instead of `-d`, `--data`, or `--data-ascii`.

The latter options don't preserve carriage returns and newlines.
{{< /admonition >}}

#### Example request body

```yaml
name: MyGroupName
rules:
  - alert: MyAlertName
    expr: up == 0
    labels:
      severity: warning
```

### Delete rule group

```
DELETE /<prometheus-http-prefix>/config/v1/rules/{namespace}/{groupName}
```

Deletes a rule group by namespace and group name. This endpoints returns `202` on success.
Escape the `{namespace}` and `{groupName}` path segments using percent-encoding, as defined by [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).
For example, escape `/` to `%2F`.

This endpoint can be disabled via the `-ruler.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

{{< admonition type="note" >}}
To delete a rule group from Mimir, use [`mimirtool rules delete` command]({{< relref "../../manage/tools/mimirtool#delete-rule-group" >}}).
{{< /admonition >}}

### Delete namespace

```
DELETE /<prometheus-http-prefix>/config/v1/rules/{namespace}
```

Deletes all the rule groups in a namespace (including the namespace itself). This endpoint returns `202` on success.
Escape the `{namespace}` path segment using percent-encoding, as defined by [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986).
For example, escape `/` to `%2F`.

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

This endpoint can be enabled and disabled via the `-alertmanager.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

{{< admonition type="note" >}}
To retrieve a tenant's Alertmanager configuration from Mimir, use [`mimirtool alertmanager get` command]({{< relref "../../manage/tools/mimirtool#get-alertmanager-configuration" >}}).
{{< /admonition >}}

### Set Alertmanager configuration

```
POST /api/v1/alerts
```

Stores or updates the Alertmanager configuration for the authenticated tenant. The Alertmanager configuration is stored in the configured backend object storage.

This endpoint expects the Alertmanager **YAML** configuration in the request body and returns `201` on success.

The names of the templates in `template_files` must be valid file names and not contain any path separators. For example, both `/templates/my-template.tpl` and `./my-template.tpl` are invalid, whereas `my-template.tpl` is valid.

This endpoint can be enabled and disabled via the `-alertmanager.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

{{< admonition type="note" >}}
To load a tenant's Alertmanager configuration to Mimir, use [`mimirtool alertmanager load` command]({{< relref "../../manage/tools/mimirtool#load-alertmanager-configuration" >}}).
{{< /admonition >}}

{{< admonition type="note" >}}
When using `curl` to send the request body from a file, ensure that you use the `--data-binary` flag instead of `-d`, `--data`, or `--data-ascii`.

The latter options don't preserve carriage returns and newlines.
{{< /admonition >}}

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

This endpoint can be enabled and disabled via the `-alertmanager.enable-api` CLI flag (or its respective YAML config option).

Requires [authentication](#authentication).

{{< admonition type="note" >}}
To delete a tenant's Alertmanager configuration from Mimir, use [`mimirtool alertmanager delete` command]({{< relref "../../manage/tools/mimirtool#delete-alertmanager-configuration" >}}).
{{< /admonition >}}

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

### Prepare for Shutdown

```
GET,POST,DELETE /store-gateway/prepare-shutdown
```

This endpoint changes in-memory store-gateway configuration to prepare for permanently stopping a store-gateway
instance but does not actually stop any part of the latter.

After a `POST` to the `prepare-shutdown` endpoint returns, when the store-gateway process is stopped with `SIGINT` / `SIGTERM`,
the store-gateway will be unregistered from the ring.

A `GET` to the `prepare-shutdown` endpoint returns the status of this configuration, either `set` or `unset`.

A `DELETE` to the `prepare-shutdown` endpoint reverts the configuration of the store-gateway to its previous state
(with respect to unregistering).

This API endpoint is usually used by Kubernetes-specific scale down automations such as the
[rollout-operator](https://github.com/grafana/rollout-operator).

## Compactor

### Compactor ring status

```
GET /compactor/ring
```

Displays a web page with the compactor hash ring status, including the state, healthy and last heartbeat time of each compactor.

### Start block upload

```
POST /api/v1/upload/block/{block}/start
```

Starts the uploading of a TSDB block with a given ID to object storage. The client should send the block's
`meta.json` file as the request body. If the complete block already exists in object storage, a
`409` (Conflict) status code gets returned. If the provided `meta.json` file is invalid, a `400` (Bad Request)
status code gets returned. If the block's max time is before the tenant's retention period, a
`422` (Unprocessable Entity) status code gets returned.

The provided `meta.json` file must have a `thanos.files` section with the list of the block's files,
otherwise the request will be rejected.

If the API request succeeds, a sanitized version of the block's `meta.json` file gets uploaded to object storage as
`uploading-meta.json`, and a `200` status code gets returned. Then you can start uploading files, and once
done, you can request completion of the block upload.

Requires [authentication](#authentication).

### Upload block file

```
POST /api/v1/upload/block/{block}/files?path={path}
```

Uploads a file with a given path, for a block with a given ID. The file path has to be one of the following,
otherwise a `400` (Bad Request) status code gets returned:

- `index`
- `chunks/<6-digit number>`

The client must send the content of the file as the body of the request; if the body is empty, a
`400` (Bad Request) status code gets returned. If the complete block already exists in object storage,
a `409` (Conflict) status code gets returned. If an in-flight meta file (`uploading-meta.json`) doesn't
exist in object storage for the block in question, a `404` (Not Found) status code gets returned.

If the API request succeeds, the file gets uploaded with the given path to the block's directory in object storage,
and a `200` status code gets returned.

Requires [authentication](#authentication).

### Complete block upload

```
POST /api/v1/upload/block/{block}/finish
```

Initiates the completion of a TSDB block with a given ID to object storage. If the complete block already
exists in object storage, a `409` (Conflict) status code gets returned. If an in-flight meta file
(`uploading-meta.json`) doesn't exist in object storage for the block in question, a `404` (Not Found)
status code gets returned. If the compactor has reached its limit for the maximum
number of concurrent block upload validations, which is configured with `-compactor.max-block-upload-validation-concurrency`,
a `429` (Too Many Requests) will be returned.

If the API request succeeds, compactor will start the block validation in the background. If the background validation
passes block upload is finished by renaming in-flight meta file to `meta.json` in the block's directory.

This API endpoint returns `200` (OK) at the beginning of the validation. To further check state of the block upload,
use [Check block upload](#check-block-upload) API endpoint.

Requires [authentication](#authentication).

This API endpoint is experimental and subject to change.

### Check block upload

```
GET /api/v1/upload/block/{block}/check
```

Returns state of the block upload. State is returned as JSON object with field `result`, with following possible values:

- `complete` -- block validation is complete, and block upload is now finished.
- `uploading` -- block is still being uploaded, and [Complete block upload](#complete-block-upload) has not yet been called on the block.
- `validating` -- block is being validated. Validation was started by call to [Complete block upload](#complete-block-upload) API.
- `failed` -- block validation has failed. Error message is available from `error` field of the returned JSON object.

**Example response**

```json
{ "result": "uploading" }
```

**Example response**

```json
{ "result": "failed", "error": "missing index file" }
```

Requires [authentication](#authentication).

This API endpoint is experimental and subject to change.

### Tenant Delete Request

```
POST /compactor/delete_tenant
```

Request deletion of ALL tenant data for the tenant specified in the `X-Scope-OrgID` header. If authentication is disabled,
then the default `anonymous` tenant is deleted (configurable by `-auth.no-auth-tenant`).

Requires [authentication](#authentication).

### Tenant Delete Status

```
GET /compactor/delete_tenant_status
```

Returns status of tenant deletion.

#### Response schema

```json
{
  "tenant_id": "<id>",
  "blocks_deleted": true
}
```

The `blocks_deleted` field will be set to `true` if all the tenant's blocks have been deleted.

Requires [authentication](#authentication).

### Compactor tenants

```
GET /compactor/tenants
```

Displays a web page with the list of tenants that have blocks in the storage configured for the compactor.

### Compactor tenant planned jobs

```
GET /compactor/tenant/{tenant}/planned_jobs
```

Displays a web page listing planned compaction jobs computed from the bucket index for the given tenant.

## Overrides-exporter

### Overrides-exporter ring status

```
GET /overrides-exporter/ring
```

Displays a web page with the overrides-exporter hash ring status, including the state, healthy and last heartbeat time of each overrides-exporter.
The overrides-exporter ring is available only when `-overrides-exporter.ring.enabled` is set to `true`.
