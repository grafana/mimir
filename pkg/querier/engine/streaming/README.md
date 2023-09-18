This directory contains a highly experimental, very limited and likely somewhat broken PromQL engine.

For more information about this engine, check out the 2023 PromCon talk ["Yet Another Streaming PromQL Engine"](https://promcon.io/2023-berlin/talks/yet-another-streaming-promql-engine).

# Supported features

The following features are supported for float samples (ie. not native histograms):

* Vector selectors (eg. `some_metric{label="value"}`)
* `sum` aggregations (eg. `sum(some_metric{label="value"})` or `sum by (group) (some_metric{label="value"})`)
  * `without` is not supported 
* `rate` function (eg. `rate(some_metric[5m])`)
* Combinations of `sum` and `rate` (eg. `sum by (group) (rate(some_metric[5m]))`)

All other PromQL features and constructs are currently unsupported, including support for native histograms.
