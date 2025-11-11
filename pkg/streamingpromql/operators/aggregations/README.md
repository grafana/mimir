# Aggregation Operations

This document provides developer-level notes explaining how aggregation operators differ from standard operator execution within the Mimir Query Engine (MQE).

## SeriesMetadata & NextSeries

MQE operators include the following lifecycle:

1. **SeriesMetadata()** — Determines which series (label sets) are likely to appear in the query results.
2. **NextSeries()** — For each series returned by `SeriesMetadata()`, retrieves the actual data points and performs any transformations before returning the result.

Note that additional label matchers applied during query evaluation may further restrict the set of returned series.

### Aggregation Exceptions

Certain aggregation operators — `topk`, `bottomk`, and `count_values` — diverge from this pattern.

These operators require analysis of all input series and their results before the series metadata can be determined, so they perform data fetching and aggregation within their `SeriesMetadata()` implementation.

In these cases, `SeriesMetadata()` queries, aggregates, and prepares all data up front, and subsequent calls to `NextSeries()` simply return the precomputed results.
