# Aggregation Operations

This document provides developer-level notes explaining how aggregation operators differ from standard operator execution within the Mimir Query Engine (MQE).

## SeriesMetadata & NextSeries

MQE operators include the following lifecycle:

1. **`SeriesMetadata()`** — Determines which series appear in this operator's result. Some of these series may return no samples from `NextSeries()`.
2. **`NextSeries()`** — Computes the samples for the next series (in the same order as returned by `SeriesMetadata()`), and performs any aggregations or transformations before returning the samples for the next series. `NextSeries()` might call `NextSeries()` multiple times on the inner operator, for example if the next output series is the sum of many input series that have not already been read.

Note that additional label matchers applied during query evaluation may further restrict the set of returned series.

### Aggregation Exceptions

Certain aggregation operators — `topk`, `bottomk`, and `count_values` — diverge from this pattern.

These operators require analysis of all input series and their results before the series metadata can be determined, so they perform data fetching and aggregation within their `SeriesMetadata()` implementation.

In these cases, `SeriesMetadata()` queries, aggregates, and prepares all data up front, and subsequent calls to `NextSeries()` simply return the precomputed results.

It should be noted that this is not the preferred implementation pattern. SeriesMetadata() retains more data in memory as it iterates over all series, which can increase memory pressure. Loading and returning data through `NextSeries()` is preferred, as it allows the operator’s consumer to process each dataset incrementally and release references or reclaim memory as soon as possible.
