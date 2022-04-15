---
title: "Grafana mimir-continuous-test"
menuTitle: "Mimir-continuous-test"
description: "Use mimir-continuous-test to continuously run smoke tests on live Grafana Mimir clusters."
weight: 30
---

# Grafana mimir-continuous-test

Mimir-continuous-test is a standalone tool that you can use to continuously run smoke tests on live Grafana Mimir clusters.
This tool targets Mimir developers.
This tool aims to help identifying a class of bugs that could be difficult to spot during development with unit and integration tests.

## Download mimir-continuous-test

- Using Docker:

```bash
docker pull "grafana/mimir-continuous-test:latest"
```

- Using a local binary:

Download the appropriate [release asset](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture and make it executable.

For Linux with the AMD64 architecture, execute the following command:

```bash
curl -Lo mimir-continuous-test https://github.com/grafana/mimir/releases/latest/download/mimir-continuous-test-linux-amd64
chmod +x mimir-continuous-test
```

## Configure mimir-continuous-test

Mimir-continuous-test requires the endpoints of the backend Grafana Mimir clusters and the tenant ID for writing and querying testing metrics:

- Set `-tests.write-endpoint` to the base endpoint on the write path. The URL should have no trailing slash. The specific API path is appended by the tool to the URL, for example `/api/v1/push` for the remote write API.
- Set `-tests.read-endpoint` to the base endpoint on the read path. The URL should have no trailing slash. The specific API path is appended by the tool to the URL, for example `/api/v1/query_range` for range query API.
- Set `-tests.tenant-id` to the tenant ID to use to write and read metrics in tests.

> **Note:** You can run `mimir-continuous-test -help` to list all available configuration options.

## How it works

Mimir-continuous-test periodically runs a suite of tests writing data to Mimir, querying it back, and checking if the query results match the expected ones.
The tool exposes metrics that can be used to alert on test failures.
Details about the failed tests are logged.

### Exported metrics

Mimir-continuous-test exposes the following Prometheus metrics at the `/metrics` endpoint listening on the port configured via the flag `-server.metrics-port`:

```bash
# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
# TYPE mimir_continuous_test_writes_total counter
mimir_continuous_test_writes_total{test="<name>"}
{test="<name>"}

# HELP mimir_continuous_test_writes_failed_total Total number of failed write requests.
# TYPE mimir_continuous_test_writes_failed_total counter
mimir_continuous_test_writes_failed_total{test="<name>",status_code="<code>"}

# HELP mimir_continuous_test_queries_total Total number of attempted query requests.
# TYPE mimir_continuous_test_queries_total counter
mimir_continuous_test_queries_total{test="<name>"}

# HELP mimir_continuous_test_queries_failed_total Total number of failed query requests.
# TYPE mimir_continuous_test_queries_failed_total counter
mimir_continuous_test_queries_failed_total{test="<name>"}

# HELP mimir_continuous_test_query_result_checks_total Total number of query results checked for correctness.
# TYPE mimir_continuous_test_query_result_checks_total counter
mimir_continuous_test_query_result_checks_total{test="<name>"}

# HELP mimir_continuous_test_query_result_checks_failed_total Total number of query results failed when checking for correctness.
# TYPE mimir_continuous_test_query_result_checks_failed_total counter
mimir_continuous_test_query_result_checks_failed_total{test="<name>"}
```

### Alerts

The released [Mimir alerts]({{< relref "../visualizing-metrics/installing-dashboards-and-alerts.md" >}}) include checks on failures tracked by mimir-continuous-test.
We recommend using the provided alerts when running mimir-continuous-test.
