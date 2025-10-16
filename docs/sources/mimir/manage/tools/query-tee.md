---
aliases:
  - ../../operators-guide/tools/query-tee/
description: Use query-tee to compare query results and performance between two Grafana Mimir clusters.
menuTitle: Query-tee
title: Grafana Mimir query-tee
weight: 30
---

# Grafana Mimir query-tee

The query-tee is a standalone tool that you can use for testing purposes when comparing the query results and performance of two Grafana Mimir clusters.
The two Mimir clusters compared by the query-tee must ingest the same series and samples.

The query-tee exposes Prometheus-compatible read API endpoints and acts as a proxy.
When the query-tee receives a request, it performs the same request against the two backend Grafana Mimir clusters and tracks the response time of each backend, and compares the query results.

## Download the query-tee

- Using Docker:

```bash
docker pull "grafana/query-tee:latest"
```

- Using a local binary:

Download the appropriate [release asset](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture and make it executable.

For Linux with the AMD64 architecture, execute the following command:

```bash
curl -Lo query-tee https://github.com/grafana/mimir/releases/latest/download/query-tee-linux-amd64
chmod +x query-tee
```

## Configure the query-tee

You can configure the query-tee using either command-line flags or a YAML configuration file. The YAML configuration provides more advanced features including per-backend request headers and individual request proportions.

### Command-line configuration

The query-tee requires the endpoints of the backend Grafana Mimir clusters.
You can configure the backend endpoints by setting the `-backend.endpoints` flag to a comma-separated list of HTTP or HTTPS URLs.

For each incoming request, the query-tee clones the request and sends it to each configured backend.

{{< admonition type="note" >}}
You can configure the query-tee proxy listening ports via the `-server.http-service-port` flag for the HTTP port and `server.grpc-service-port` flag for the gRPC port.
{{< /admonition >}}

### YAML configuration

For advanced configurations, you can use a YAML configuration file by specifying the `-config.file` flag:

```bash
query-tee -config.file=config.yaml
```

The YAML configuration supports all command-line options plus additional per-backend features:

```yaml
# Server configuration
server:
  http_service:
    address: ""           # HTTP bind address (default: all interfaces)
    port: 8080           # HTTP port (default: 80)
  grpc_service:
    address: ""           # gRPC bind address (default: all interfaces)
    port: 9095           # gRPC port (default: 9095)
  graceful_shutdown_timeout: 30s  # Graceful shutdown timeout
  path_prefix: ""       # Path prefix for API routes

# Backend endpoints configuration
backends:
  endpoints:
    - name: "preferred-backend"
      url: "http://mimir-1.example.com:8080"
      preferred: true               # Exactly one backend must be preferred
      timeout: 150s                 # Request timeout (default: 150s)
      skip_tls_verify: false        # Skip TLS verification
      request_headers:              # Custom headers for this backend
        X-Scope-OrgID: ["tenant-1"]
        Authorization: ["Bearer token123"]
    - name: "secondary-backend"
      url: "http://mimir-2.example.com:8080"
      preferred: false
      request_proportion: 0.8       # Send 80% of requests to this backend
      request_headers:
        X-Scope-OrgID: ["tenant-2"]

# Proxy behavior configuration
proxy:
  compare_responses: false                              # Compare responses between backends
  passthrough_non_registered_routes: false             # Pass unregistered routes to preferred backend
  add_missing_time_parameter_to_instant_queries: true  # Add time param to instant queries
  log_slow_query_response_threshold: 10s               # Log slow query threshold
  skip_preferred_backend_failures: false               # Skip comparisons when preferred fails

# Response comparison configuration
comparison:
  value_tolerance: 0.000001         # Floating point comparison tolerance
  use_relative_error: false         # Use relative vs absolute error tolerance
  skip_recent_samples: 2m           # Skip samples within this time window
  skip_samples_before: ""           # Skip samples before this timestamp (RFC3339)
  require_exact_error_match: false  # Require exact error message matching
```

#### Environment variable expansion

The YAML configuration supports environment variable expansion using the `-config.expand-env` flag:

```bash
query-tee -config.file=config.yaml -config.expand-env=true
```

In your YAML file, you can use `${VAR}` or `$VAR` syntax:

```yaml
backends:
  endpoints:
    - name: "backend1"
      url: "${BACKEND1_URL}"
      request_headers:
        Authorization: ["Bearer ${API_TOKEN}"]
```

### Usage examples

#### Basic comparison setup

Command-line setup for basic comparison between two clusters:

```bash
query-tee \
  -backend.endpoints="http://old-mimir:8080,http://new-mimir:8080" \
  -backend.preferred="old-mimir" \
  -proxy.compare-responses=true \
  -server.http-service-port=8080
```

#### Multi-tenant YAML configuration

YAML configuration for testing multiple tenants with different backends:

```yaml
# config.yaml
server:
  http_service:
    port: 8080
    
backends:
  endpoints:
    - name: "production"
      url: "http://prod-mimir.company.com:8080"
      preferred: true
      request_headers:
        X-Scope-OrgID: ["prod-tenant"]
        
    - name: "staging-canary"
      url: "http://staging-mimir.company.com:8080"
      preferred: false
      request_proportion: 0.1  # Only 10% of traffic for canary testing
      request_headers:
        X-Scope-OrgID: ["staging-tenant"]
        
    - name: "development"
      url: "http://dev-mimir.company.com:8080"
      preferred: false
      request_proportion: 0.05  # Only 5% of traffic for dev testing
      request_headers:
        X-Scope-OrgID: ["dev-tenant"]
        X-Debug-Mode: ["true"]

proxy:
  compare_responses: true
  log_slow_query_response_threshold: 5s

comparison:
  value_tolerance: 0.001
  skip_recent_samples: 3m
```

Run with:

```bash
query-tee -config.file=config.yaml
```

## How the query-tee works

This section describes how the query-tee tool works.

### API endpoints

Query-tee accepts two types of requests:

1. HTTP requests on the configured `-server.http-service-port` flag (default port 80)
1. [HTTP over gRPC](https://github.com/weaveworks/common/tree/master/httpgrpc) requests on the configured `-server.grpc-service-port` flag (default port: 9095)

The following Prometheus API endpoints are supported by `query-tee`:

- `GET <prefix>/api/v1/query`
- `GET <prefix>/api/v1/query_range`
- `GET <prefix>/api/v1/query_exemplars`
- `GET <prefix>/api/v1/labels`
- `GET <prefix>/api/v1/label/{name}/values`
- `GET <prefix>/api/v1/series`
- `GET <prefix>/api/v1/metadata`
- `GET <prefix>/api/v1/alerts`
- `GET <prefix>/prometheus/config/v1/rules`

You can configure the `<prefix>` by setting the `-server.path-prefix` flag, which defaults to an empty string.

### Pass-through requests

The query-tee can optionally act as a transparent proxy for requests to routes not matching any of the supported API endpoints.
You can enable the pass-through support setting `-proxy.passthrough-non-registered-routes=true` and configuring a preferred backend using the `-backend.preferred` flag.
When pass-through is enabled, a request for an unsupported API endpoint is transparently proxied to the configured preferred backend.

### Authentication

The query-tee supports [HTTP basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication).
The query-tee can merge the HTTP basic authentication in the received request with the username and configured in a backend URL.

A request sent from the query-tee to a backend includes HTTP basic authentication when one of the following conditions is met:

- If the backend endpoint URL is configured with both a username and password, then query-tee uses it.
- If the backend endpoint URL is configured only with a username, then query-tee keeps the configured username and injects the password received in the incoming request.
- If the backend endpoint URL is configured without a username and password, then query-tee forwards the authentication credentials found in the incoming request.

#### Custom request headers

When using YAML configuration, you can configure custom HTTP headers to be sent to specific backends:

```yaml
backends:
  endpoints:
    - name: "tenant-1-backend"
      url: "http://mimir-tenant1.example.com:8080"
      preferred: true
      request_headers:
        X-Scope-OrgID: ["tenant-1"]
        Authorization: ["Bearer eyJ0eXAiOiJKV1QiLCJhbGc..."]
        X-Custom-Header: ["value1", "value2"]  # Multiple values supported
    - name: "tenant-2-backend"
      url: "http://mimir-tenant2.example.com:8080"
      preferred: false
      request_headers:
        X-Scope-OrgID: ["tenant-2"]
        Authorization: ["Bearer eyJ0eXAiOiJKV1QiLCJhbGc..."]
```

Custom headers are useful for:
- **Multi-tenancy**: Send different tenant IDs to different backends
- **Authentication**: Use different API tokens or credentials for each backend
- **Debugging**: Add tracing or debugging headers to specific backends
- **Load balancing**: Add custom routing headers for backend infrastructure

{{< admonition type="note" >}}
Custom headers are added to outgoing requests to backends. They are combined with any headers from the original client request, with custom headers taking precedence over client headers for the same header name.
{{< /admonition >}}

### Backend selection

You can use the query-tee to either send requests to all backends, or to send a proportion of requests to specific backends.

#### Command-line configuration

You can configure a global proportion for all secondary backends using the `-proxy.secondary-backends-request-proportion` CLI flag.

For example, if you set the `-proxy.secondary-backends-request-proportion` CLI flag to `1.0`, then all requests are sent to all backends.
Alternatively, if you set the `-proxy.secondary-backends-request-proportion` CLI flag to `0.2`, then 20% of requests are sent to all backends, and the remaining 80% of requests are sent only to your preferred backend.

#### YAML configuration

With YAML configuration, you can set individual request proportions for each secondary backend:

```yaml
backends:
  endpoints:
    - name: "preferred"
      url: "http://primary.example.com:8080"
      preferred: true
      # Preferred backend always receives 100% of requests
    - name: "secondary-canary"
      url: "http://canary.example.com:8080"
      preferred: false
      request_proportion: 0.1  # Send 10% of requests to canary
    - name: "secondary-full"
      url: "http://secondary.example.com:8080"
      preferred: false
      request_proportion: 1.0  # Send 100% of requests to secondary
```

This configuration sends:
- 100% of requests to the preferred backend
- 10% of requests to the canary backend
- 100% of requests to the full secondary backend

{{< admonition type="note" >}}
The `request_proportion` field only applies to secondary backends (where `preferred: false`). The preferred backend always receives all requests.
{{< /admonition >}}

### Backend response selection

The query-tee enables you to configure a preferred backend that selects the response to send back to the client.
The query-tee returns the `Content-Type` header, HTTP status code, and body of the response from the preferred backend.
The preferred backend can be configured via `-backend.preferred=<hostname>`.
The value of the preferred backend configuration option must be the hostname of one of the configured backends.

When a preferred backend is configured, the query-tee always returns the response from the preferred backend.

When a preferred backend is not configured, the query-tee uses the following algorithm to select the backend response to send back to the client:

1. If at least one backend response status code is 2xx or 4xx, the query-tee selects the first received response whose status code is 2xx or 4xx.
1. If no backend response status code is 2xx or 4xx, the query-tee selects the first received response regardless of the status code.

{{< admonition type="note" >}}
The query-tee considers a 4xx response as a valid response to select because a 4xx status code is generally an invalid request and not a server-side issue.
{{< /admonition >}}

### Backend results comparison

You can use the query-tee to compare query results received from multiple backends.
The query results comparison can be enabled setting the flag `-proxy.compare-responses=true` and requires that:

1. You've configured at least two backends by setting `-backend.endpoints`.
1. You've configured a preferred backend by setting `-backend.preferred`.

When you enable the query results comparison, the query-tee compares the response received from the preferred backend against each secondary backend individually and logs a message for each query whose results don't match. Query-tee keeps track of the number of successful and failed comparison through the metric `cortex_querytee_responses_compared_total`, with separate metrics for each secondary backend.

By default, query-tee considers equivalent error messages as matching, even if they are not exactly the same.
This ensures that comparison does not fail for known situations where error messages are non-deterministic.
Set `-proxy.compare-exact-error-matching=true` to require that error messages match exactly.

{{< admonition type="note" >}}
Query-tee compares floating point sample values with a tolerance that you can configure with the `-proxy.value-comparison-tolerance` option.

The configured tolerance prevents false positives due to differences in floating point values rounding introduced by the non-deterministic series ordering within the Prometheus PromQL engine.
{{< /admonition >}}

{{< admonition type="note" >}}
The default value of `-proxy.compare-skip-recent-samples` is two minutes.
This means points within results with a timestamp within two minutes of the current time aren't compared.
This prevents false positives due to racing with ingestion, and, if the query selects the output of recording rules, rule evaluation.

If either Mimir cluster is running with a non-default value of `-ruler.evaluation-delay-duration`, you should set `-proxy.compare-skip-recent-samples` to one minute more than the value of `-ruler.evaluation-delay-duration`.
{{< /admonition >}}

### Slow query log

You can configure query-tee to log requests that take longer than the fastest backend by setting the flag `-proxy.log-slow-query-response-threshold`.

The default value is `10s` which logs requests that are ten seconds slower than the fastest backend.

To disable slow query logging, set `-proxy.log-slow-query-response-threshold` to `0`.

### Exported metrics

The query-tee exposes the following Prometheus metrics at the `/metrics` endpoint listening on the port configured via the flag `-server.metrics-port`:

```bash
# HELP cortex_querytee_backend_request_duration_seconds Time (in seconds) spent serving requests.
# TYPE cortex_querytee_backend_request_duration_seconds histogram
cortex_querytee_backend_request_duration_seconds_bucket{backend="<hostname>",method="<method>",route="<route>",status_code="<status>",le="<bucket>"}
cortex_querytee_backend_request_duration_seconds_sum{backend="<hostname>",method="<method>",route="<route>",status_code="<status>"}
cortex_querytee_backend_request_duration_seconds_count{backend="<hostname>",method="<method>",route="<route>",status_code="<status>"}

# HELP cortex_querytee_responses_total Total number of responses sent back to the client by the selected backend.
# TYPE cortex_querytee_responses_total counter
cortex_querytee_responses_total{backend="<hostname>",method="<method>",route="<route>"}

# HELP cortex_querytee_responses_compared_total Total number of responses compared per route name by result.
# TYPE cortex_querytee_responses_compared_total counter
cortex_querytee_responses_compared_total{route="<route>",secondary_backend="<hostname>",result="<success|fail|skip>"}
```

Additionally, if backend results comparison is configured, two native histograms are available:

- `cortex_querytee_backend_response_relative_duration_seconds{route="<route>",secondary_backend="<hostname>"}`: Time (in seconds) of the secondary backend minus the preferred backend, for each secondary backend.
- `cortex_querytee_backend_response_relative_duration_proportional{route="<route>",secondary_backend="<hostname>"}`: Response time of the secondary backend minus the preferred backend, as a proportion of the preferred backend response time.

### Ruler remote operational mode test

When the ruler is configured with the [remote evaluation mode](../../../references/architecture/components/ruler/) you can use the query-tee to compare rule evaluations too.
To test ruler evaluations with query-tee, set the `-ruler.query-frontend.address` CLI flag or its respective YAML configuration parameter for the ruler with query-tee's gRPC address:

```
ruler:
  query_frontend:
    address: "dns://query-tee:9095"
```

When the ruler evaluates a rule, the test flow is the following:

1. ruler sends gRPC request to query-tee
1. query-tee forwards the request to the query-frontend backends configured setting the `-backend.endpoints` CLI flag
1. query-tee receives the response from the query-frontend and forwards the result (based on the preferred backend) to the ruler
