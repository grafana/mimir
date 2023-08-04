---
description: Learn how to query metric labels from Grafana Mimir.
title: Query metric labels
menuTitle: Query metric labels
weight: 2
keywords:
  - labels
---

# Query metric labels

Grafana Mimir supports multiple [HTTP API]({{< relref "../references/http-api/index.md" >}}) endpoints to query metric label names and values.
In this document we explain the main differences and trade-off between these API endpoints.

## Querying label names

The following API endpoints can be used to find the list of label names of all (or a subset of) series stored in Mimir:

- **[Get label names]({{< relref "../references/http-api/index.md#get-label-names" >}})**<br />
  `GET,POST <prometheus-http-prefix>/api/v1/labels`
- **[Label names cardinality]({{< relref "../references/http-api/index.md#label-names-cardinality" >}})**<br />
  `GET,POST <prometheus-http-prefix>/api/v1/cardinality/label_names`

> **Note**: `<prometheus-http-prefix>` is the Prometheus HTTP prefix as documented in the [HTTP API]({{< relref "../references/http-api/index.md#path-prefixes" >}}) reference.
> The default prefix is `/prometheus`.

### Recommendations and performance trade-off

- Prefer "get label names" over "label names cardinality" API, unless you need to know the number of unique label values for each label name.
- The "get label names" API `start` and `end` parameters are optional. When omitted, Mimir will query label names for the whole retention period or up to the configured `-store.max-labels-query-length`. We recommend to always specify `start` and `end` parameters, and preferably set to the shortest period which is feasible for your use case.
- For both API endpoints, executing a request specifying the series selector is computationally more expensive than the same request without the series selector, but the results set can be significantly smaller when the series selector effectively reduce the series set.

### Features comparison

The different API endpoints have different features.
The following list summarises the main differences.

#### Can specify the time range in the request?

- **Get label names**<br />
  Yes. However, the actual time range granularity is restricted to TSDB block range periods (2h for the most recent metrics and up to 24h for historical metrics when running Mimir with the default configuration). In practice, the input `start` and `end` parameters are rounded to the TSDB block boundaries: the `start` parameter is rounded to the start of the nearest block containing samples whose timestamp is lower or equal than the input timestamp, while the `end` parameter is rounded to the end of the nearest block containing samples with timestamp greater or equal than the input timestamp.
- **Label names cardinality**<br />
  No. The API queries only the in-memory series in the ingesters that hold series for the tenant.

#### Can specify the series selector in the request?

- **Get label names API**<br />
  Yes. The optional `match[]` parameter allows to select the series from which to read the label names. The `match[]` parameter can be specified multiple times. When omitted, label names are extracted from all series.
- **Label names cardinality API**<br />
  Yes. The optional `selector` parameter allows to select the series from which to read the label names. The `selector` parameter can be specified only once. When omitted, label names are extracted from all series.

## Querying label values

The following API endpoints can be used to find the list of label values for a given label name:

- **[Get label values]({{< relref "../references/http-api/index.md#get-label-values" >}})**<br />
  `GET <prometheus-http-prefix>/api/v1/label/{name}/values`
- **[Label values cardinality]({{< relref "../references/http-api/index.md#label-values-cardinality" >}})**<br />
  `GET,POST <prometheus-http-prefix>/api/v1/cardinality/label_values`

> **Note**: `<prometheus-http-prefix>` is the Prometheus HTTP prefix as documented in the [HTTP API]({{< relref "../references/http-api/index.md#path-prefixes" >}}) reference.
> The default prefix is `/prometheus`.

### Recommendations and performance trade-off

- Prefer "get label values" over "label values cardinality" API, unless you need to know the number of series for each label name and value.
- The "get label values" API allows to specify a single label name, while the "label values cardinality" API allows to specify multiple label names. If you need to get the label values for multiple label names, we recommend issuing multiple "get label values" requests instead of a single "label values cardinality" API call with multiple label names.
- The "get label values" API `start` and `end` parameters are optional. When omitted, Mimir will query label values for the whole retention period or up to the configured `-store.max-labels-query-length`. We recommend to always specify `start` and `end` parameters, and preferably set it to the shortest period which is feasible for your use case. The longer the time range, the more computationally expensive the request is.
- For both API endpoints, executing a request specifying the series selector is computationally more expensive than the same request without the series selector, but the results set can be significantly smaller when the series selector effectively reduce the series set.

### Features comparison

The different API endpoints have different features.
The following list summarises the main differences.

#### Can specify the time range in the request?

- **Get label values API**<br />
  Yes. However, the actual time range granularity is restricted to TSDB block range periods (2h for the most recent metrics and up to 24h for historical metrics when running Mimir with the default configuration). In practice, the input `start` and `end` parameters are rounded to the TSDB block boundaries: the `start` parameter is rounded to the start of the nearest block containing samples whose timestamp is lower or equal than the input timestamp, while the `end` parameter is rounded to the end of the nearest block containing samples with timestamp greater or equal than the input timestamp.
- **Label values cardinality API**<br />
  No. The API queries only the in-memory series in the ingesters that hold series for the tenant.

#### Can specify the series selector in the request?

- **Get label values API**<br />
  Yes. The optional `match[]` parameter allows to select the series from which to read the label values. The `match[]` parameter can be specified multiple times. When omitted, label values are extracted from all series.
- **Label values cardinality API**<br />
  Yes. The optional `selector` parameter allows to select the series from which to read the label names and values. The `selector` parameter can be specified only once. When omitted, label names and values are extracted from all series.

## Alternatives

The following API endpoints have not been explicitly designed to query metric label names and values, but label names and/or values can be extracted from the response:

- **[Get series by label matchers]({{< relref "../references/http-api/index.md#get-series-by-label-matchers" >}})**<br />
  `GET,POST <prometheus-http-prefix>/api/v1/series`
- **[Instant query]({{< relref "../references/http-api/index.md#instant-query" >}})**<br />
  `GET,POST <prometheus-http-prefix>/api/v1/query`

> **Note**: `<prometheus-http-prefix>` is the Prometheus HTTP prefix as documented in the [HTTP API]({{< relref "../references/http-api/index.md#path-prefixes" >}}) reference.
> The default prefix is `/prometheus`.

### Recommendations and performance trade-off

- Prefer "get label names" over other APIs to get the list of label names. The "get label names" is expected to always perform better than other APIs.
- Prefer "get label values" over other APIs to get the list of label values.
  - If you're experiencing a slow response time when querying "get label values" API, the request includes a series selector and the series selector is expected to match a small set of series (order of few thousands), then consider using "get series by label matchers" instead of the "get label values" API.
  - If you can't use "get series by label matchers" API (e.g. the API is not available in the client you're using), then fallback to the "instant query" API.
