---
title: "Query-tee"
description: ""
weight: 10
---

# Query-tee

The query-tee is a standalone tool that you can use for testing purposes when comparing the query results and performances of two Grafana Mimir clusters.
The two Mimir clusters compared by the query-tee must ingest the same exact series and samples.

The query-tee exposes Prometheus-compatible read API endpoints and acts as a proxy.
When the query-tee receives a request, the query-tee performs the same request against the two backend Grafana Mimir clusters and tracks the response time of each backend, and compares the query results.

## How to download the query-tee

- Using Docker:

```bash
docker pull "grafana/query-tee:latest"
```

- Using a local binary:

Download the appropriate [release asset](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture and make it executable.
For Linux with the AMD64 architecture:

```bash
curl -Lo query-tee https://github.com/grafana/mimir/releases/latest/download/query-tee-linux-amd64
chmod +x query-tee
```

## How to configure the query-tee

The query-tee requires the endpoints of the backend Grafana Mimir clusters.
You can configure the backend endpoints by setting the `-backend.endpoints` CLI flag to a comma-separated list of HTTP or HTTPS URLs.
For each incoming request, the query-tee clones the request and sends it to each configured backend.

> **Note:** You can configure the query-tee proxy listening port with the `-server.service-port` CLI flag.

## How the query-tee works

### API endpoints

The following Prometheus API endpoints are supported by `query-tee`:

- `GET <prefix>/api/v1/query`
- `GET <prefix>/api/v1/query_range`
- `GET <prefix>/api/v1/query_exemplars`
- `GET <prefix>/api/v1/labels`
- `GET <prefix>/api/v1/label/{name}/values`
- `GET <prefix>/api/v1/series`
- `GET <prefix>/api/v1/metadata`
- `GET <prefix>/api/v1/alerts`
- `GET <prefix>/api/v1/rules`

You can configure the `<prefix>` by setting the `-server.path-prefix` CLI flag (defaults to empty string).

### Pass-through requests

The query-tee can optionally act as a transparent proxy for requests to routes not matching any of the supported API endpoints.
You can enable the pass-through support setting `-proxy.passthrough-non-registered-routes=true` and configuring a preferred backend using the `-backend.preferred` CLI flag.
When pass-through is enabled, a request for an unsupported API endpoint is transparently proxied to the configured preferred backend.

### Authentication

The query-tee supports [HTTP basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication).
The query-tee can merge the HTTP basic authentication in the received request with the username and configured in a backend URL.

A request sent from the query-tee to a backend includes HTTP basic authentication when one of the following conditions are met:

- If the backend endpoint URL is configured with both a username and password, then query-tee uses it.
- If the backend endpoint URL is configured only with a username, then query-tee keeps the configured username and inject the password received in the incoming request.
- If the backend endpoint URL is configured without username and password, then query-tee forwards the authentication credentials found in the incoming request.

### Backend response selection

The query-tee allows to configure a preferred backend from which selecting the response to send back to the client.
The preferred backend can be configured using the CLI flag `-backend.preferred=<hostname>`.
The value of the preferred backend configuration option must be the hostname of one of the configured backends.

When a preferred backend is configured, the query-tee uses the following algorithm to select the backend response to send back to the client:

1. If the preferred backend response status code is 2xx or 4xx, the query-tee selects the response from the preferred backend.
1. If at least one backend response has status code 2xx or 4xx, the query-tee selects the first received response whose status code is 2xx or 4xx.
1. If no backend response has status code 2xx or 4xx, the query-tee selects the first received response regardless of the status code.

When a preferred backend is not configured, the query-tee uses the following algorithm to select the backend response to send back to the client:

1. If at least one backend response has status code 2xx or 4xx, the query-tee selects the first received response whose status code is 2xx or 4xx.
1. If no backend response has status code 2xx or 4xx, the query-tee selects the first received response regardless of the status code.

> **Note:** The query-tee considers a 4xx response as a valid response to select because a 4xx status code generally means the error is caused by an invalid request and not due to a server side issue.

### Backend results comparison

The query-tee can optionally compare the query results received by two backends.
The query results comparison can be enabled setting the CLI flag `-proxy.compare-responses=true` and requires that:

1. Exactly two backends have been configured setting `-backend.endpoints`.
1. A preferred backend is configured setting `-backend.preferred`.

When the query results comparison is enabled, the query-tee compares the response received from the two configured backends and logs a message for each query whose results don't match, and keeps track of the number of successful and failed comparison through the metric `cortex_querytee_responses_compared_total`.

> **Note**: Floating point sample values are compared with a tolerance that can be configured via `-proxy.value-comparison-tolerance`. The configured tolerance prevents false positives due to differences in floating point values rounding introduced by the non deterministic series ordering within the Prometheus PromQL engine.

### Exported metrics

The query-tee exposes the following Prometheus metrics at the `/metrics` endpoint listening on the port configured via the CLI flag `-server.metrics-port`:

```bash
# HELP cortex_querytee_request_duration_seconds Time (in seconds) spent serving HTTP requests.
# TYPE cortex_querytee_request_duration_seconds histogram
cortex_querytee_request_duration_seconds_bucket{backend="<hostname>",method="<method>",route="<route>",status_code="<status>",le="<bucket>"}
cortex_querytee_request_duration_seconds_sum{backend="<hostname>",method="<method>",route="<route>",status_code="<status>"}
cortex_querytee_request_duration_seconds_count{backend="<hostname>",method="<method>",route="<route>",status_code="<status>"}

# HELP cortex_querytee_responses_total Total number of responses sent back to the client by the selected backend.
# TYPE cortex_querytee_responses_total counter
cortex_querytee_responses_total{backend="<hostname>",method="<method>",route="<route>"}

# HELP cortex_querytee_responses_compared_total Total number of responses compared per route name by result.
# TYPE cortex_querytee_responses_compared_total counter
cortex_querytee_responses_compared_total{route="<route>",result="<success|fail>"}
```
