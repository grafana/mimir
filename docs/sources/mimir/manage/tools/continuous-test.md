---
aliases:
  - ../../operators-guide/tools/mimir-continuous-test/
  - ./mimir-continuous-test
description: Use continuous-test to continuously run smoke tests on live Grafana Mimir clusters.
menuTitle: Continuous-test
title: continuous-test
weight: 30
---

# Continuous-test

As a developer, you can use the continuous-test tool to run smoke tests on live Grafana Mimir clusters.
This tool identifies a class of bugs that could be difficult to spot during development.
Two operating modes are supported:

- As a continuously running deployment in your environment, continuous-test can be used to detect issues on a live Grafana Mimir cluster over time.
- As an ad-hoc smoke test tool, continuous-test can be used to validate basic functionality after configuration changes are made to a Grafana Mimir cluster.

## Download and run continuous-test

- Using Docker:

```bash
docker pull "grafana/mimir:latest"
docker run --rm -ti grafana/mimir -target=continuous-test
```

- Using a local binary:

Download the appropriate [mimir binary](https://github.com/grafana/mimir/releases/latest) for your operating system and architecture, and make it executable.

For Linux with the AMD64 architecture, execute the following command:

```bash
curl -Lo mimir https://github.com/grafana/mimir/releases/latest/download/mimir-linux-amd64
chmod +x mimir
mimir -target=continuous-test
```

{{< admonition type="note" >}}
In [Grafana Mimir 2.13](../../../release-notes/v2.13/) the continuous-test became a part of Mimir as its own target.

The standalone `grafana/mimir-continuous-test` Docker image and mimir-continuous-test binary were deprecated in 2.13 and removed in 3.0.
{{< /admonition >}}

## Configure continuous-test

Continuous-test requires the endpoints of the backend Grafana Mimir clusters and the authentication for writing and querying testing metrics:

- Set `-tests.write-endpoint` to the base endpoint on the write path. Remove any trailing slash from the URL. The tool appends the specific API path to the URL, for example `/api/v1/push` for the remote-write API.
- Set `-tests.read-endpoint` to the base endpoint on the read path. Remove any trailing slash from the URL. The tool appends the specific API path to the URL, for example `/api/v1/query_range` for the range-query API.
- Set the authentication means to use to write and read metrics in tests. By priority order:
  - `-tests.bearer-token` for bearer token authentication.
  - `-tests.basic-auth-user` and `-tests.basic-auth-password` for a basic authentication.
  - `-tests.tenant-id` to the tenant ID, default to `anonymous`.
- Set `-tests.smoke-test` to run the test once and immediately exit. In this mode, the process exit code is non-zero when the test fails.

{{< admonition type="note" >}}
You can run `mimir -help` to list all available configuration options. All configuration options for continuous-test begin with `tests`.
{{< /admonition >}}

## Test native OTLP delta ingestion

The experimental OTLP delta test checks that delta counters and exponential histograms retain their values and interval boundaries through ingestion and querying.
It is disabled by default and requires native delta ingestion, start-timestamp storage, and a query engine configured to use start timestamps and support `start_timestamp()`.

The following example enables the delta test alongside the standard write/read series test and sends delta metrics directly to Mimir:

```bash
mimir -target=continuous-test \
  -tests.write-endpoint=http://mimir-gateway \
  -tests.read-endpoint=http://mimir-gateway/prometheus \
  -tests.tenant-id=mimir-delta-integration \
  -tests.write-read-series-test.histogram-samples-enabled=true \
  -tests.otlp-delta-test.enabled=true
```

The delta test uses the common `-tests.write-endpoint` and appends `/otlp/v1/metrics` to the base URL.
It always writes OTLP, regardless of `-tests.write-protocol`, and uses the common tenant and authentication settings.

The delta test uses the `-tests.write-read-series-test` settings for series count, float and histogram selection, maximum query age, and query fan-out.
It generates positive sine-wave increments for monotonic delta sums and integer bucket counts for exponential delta histograms.
Each metric has a distinct, stable name, and each sample represents a 20-second interval ending at its sample timestamp.

Like the standard write/read test, the delta test fills in samples between runs and recovers previously written history after a restart.
Recovery checks raw values so that missing start timestamps do not cause the test to discard history that should be checked.
On a fresh start, it writes two samples per series so that smoke-test mode can also check `irate()`.

The test runs instant and range queries over recent and historical data, with the results cache both enabled and disabled.
These queries check:

- Raw values, sample presence, and histogram buckets.
- Start timestamps against the expected interval boundaries. Absolute errors are summed across series so that errors cannot cancel out.
- `irate()` against each interval's value divided by 20 seconds. Rates are calculated before aggregating series.

Query windows follow the standard write/read test's selection, although sample age alone does not guarantee a store-gateway read.
Query failures are reported immediately, and the exported metrics identify the delta test with `test="otlp-delta"`.
Coverage does not yet include broader `rate()` or `increase()` windows, irregular or overlapping intervals, or signed up/down counters.

## How it works

Continuous-test periodically runs a suite of tests, writes data to Mimir, queries that data back, and checks if the query results match what is expected.
The tool exposes metrics that you can use to alert on test failures. The tool logs the details about the failed tests.

### Exported metrics

Continuous-test exposes the following Prometheus metrics at the `/metrics` endpoint listening on the port that you configured via the flag `-server.metrics-port`:

```bash
# HELP mimir_continuous_test_writes_total Total number of attempted write requests.
# TYPE mimir_continuous_test_writes_total counter
mimir_continuous_test_writes_total{test="<name>"}
{test="<name>"}

# HELP mimir_continuous_test_writes_failed_total Total number of failed write requests.
# TYPE mimir_continuous_test_writes_failed_total counter
mimir_continuous_test_writes_failed_total{test="<name>",status_code="<code>"}

# HELP mimir_continuous_test_writes_request_duration_seconds Duration of the requests
# TYPE mimir_continuous_test_writes_request_duration_seconds histogram
mimir_continuous_test_writes_request_duration_seconds{test="<name>"}

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

# HELP mimir_continuous_test_queries_request_duration_seconds Duration of the requests
# TYPE mimir_continuous_test_queries_request_duration_seconds histogram
mimir_continuous_test_queries_request_duration_seconds{results_cache="false",test="<name>"}
mimir_continuous_test_queries_request_duration_seconds{results_cache="true",test="<name>"}
```

{{< admonition type="note" >}}
The metrics `mimir_continuous_test_writes_request_duration_seconds` and `mimir_continuous_test_queries_request_duration_seconds`
are exposed as [native histograms](../../../send/native-histograms/) and don’t have proper textual presentation on the `/metrics` endpoint.
{{< /admonition >}}

### Alerts

[Grafana Mimir alerts](../../monitor-grafana-mimir/installing-dashboards-and-alerts/) include checks on failures that continuous-test tracks.
When running continuous-test, use the provided alerts.
