# Query Fuzz Testing

This file contains notes related to the addition of fuzz tests for the Mimir query engine (MQE).

Fuzz tests are in the `engine_fuzz_test.go` file.

The test assertions are that the Prometheus engine returns the same result as MQE.

Considered in this test are the result errors, result values and result annotations.

There are some expected cases where the Prometheus and MQE query engines will return different responses.

It is important to be aware of these differences if you use the fuzz tool to generate new queries, as these differences will appear as test failures.

The scenarios where a difference can occur include;

- [annotation] MQE may not emit warning annotations on binary operations that produce no series - where an expression has a left and right hand side, the right hand side expression will not generate a warning if the left hand side would evaluate to having no results. For instance, in MQE the query `false / quantile_over_time(1.95, metric[5m])` would not issue a quantile value being out of range warning
- [annotation] There can be a difference in annotation messages which include the query string offset as to where the error occurs. There is a difference in query string whitespace trimming which results in the reported string offsets being different. For instance `PromQL warning: quantile value should be between 0 and 1, got 2 (1:25)` - the `(1:25)` may be different.
- [result] There can be a difference in the returned series when the `topk` and `bottomk` aggregations are used over multiple series with the same values. In this scenario the returned series are indeterminate, and the returned series from MQE may differ from that of Prometheus' engine
- [result] There are some expressions not yet supported by MQE, such as info, mad_over_time, sort_by_label and sort_by_label_desc

Further information on the differences can be found [here](../../docs/sources/mimir/references/architecture/mimir-query-engine.md#known-differences-compared-to-prometheus-engine)

If you need to exclude a test query from comparing annotations, the query can be prefixed with `[ignore_annotations]` in `seed-queries.test`, for example:

```text
# This query will compare any returned annotations
(({metric}[2y]@012))

# This query will ignore comparing returned annotations
[ignore_annotations] quAntile without () (  +(2),(A))
```

## Fuzz Usage

```
go test -fuzz=FuzzQuery .
```

When invoked the seed queries are all tested first, and then the Go fuzzer will start generating new string queries based off this seed corpus.

If a generated string fails the test assertions, its query is written to a new file in the `testdata/fuzz` directory.

These failures are always re-tested when the test cases are either run as unit tests or fuzz tests.

By default, the fuzz tests will continue until a failure is found. A time bound can be added to the fuzz run as follows;

```
go test -fuzz=FuzzQuery -fuzztime 30s .
```

## Query Generation

The `seed-queries.test` file has a number of randomly generated queries related to the seed data metrics.

Many of these have been generated using the [https://github.com/cortexproject/promqlsmith](https://github.com/cortexproject/promqlsmith) utility.

A sample tool which uses `promqlsmith` can be found in the `tools/promql-generator` director.

See the `tools/promql-generator/README.md` for examples of how to use this tool to generate promql queries.
