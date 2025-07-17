# Query Fuzz Testing

This file contains notes related to the addition of Fuzz tests for the Mimir query engine.

Fuzz tests have been added to the `engine_fuzz_test.go` file. 

The test assertions are that the Prometheus PromQL Engine returns the same result as the Mimir Query Engine. 

Considered in this test are the result errors, result values and result annotations.

There are some expected cases where the Prometheus and Mimir query engines will return different responses.

It is important to be aware of these differences if you use the Fuzz tool to generate new queries, as these differences will appear as test failures.

The scenarios where a difference can occur include;

* [annotation] Mimir may not emit warning annotations on binary operations that produce no series - where an expression has a left and right hand side, the right hand side expression will not generate a warning if the left hand side would evaluate to having no results. For instance, in Mimir engine the query  `false / quantile_over_time(1.95, metric[5m])` would not issue a quantile value being out of range warning  
* [annotation] There can be a difference in annotation messages which include the query string offset as to where the error occurs. There is a difference in query string whitespace trimming which results in the reported string offsets being different. For instance `PromQL warning: quantile value should be between 0 and 1, got 2 (1:25)` - the `(1:25)` may be different.
* [result] There can be a difference in the returned series when the `topk` and `bottomk` queries are used over a dataset which matches the same values. In this scenario the returned series is indeterminate, and the returned series from Mimir may differ from that of Prometheus 
* [result] There are some expressions not yet supported by the Mimir Query Engine, such as info, mad_over_time, sort_by_label and sort_by_label_desc

Further information on the differences can be found [here](https://github.com/grafana/mimir/blob/main/docs/sources/mimir/references/architecture/mimir-query-engine.md#known-differences-compared-to-prometheus-engine)

If you need to exclude a test query from comparing annotations, the query can be prefixed with `[ignore_annotations]` in the data seeding file.

```text
# This query will compare any returned annotations
(({metric}[2y]@012))

# This query will ignore comparing returned annotations
[ignore_annotations] quAntile without () (  +(2),(A))
```

## Fuzz Usage

```
go test -fuzz=FuzzQuery
```

When invoked the seed queries are all tested first, and then the Go Fuzzer will start generating new string queries based off this seed corpus.

If a generated string fails the test assertions, it's query is written to a new file in the `testdata/fuzz` directory. 

These failures are always re-tested when the test cases are either run as unit tests or fuzz tests.

By default, the fuzz tests will continue until a failure is found. A time bound can be added to the fuzz run as follows;

```
go test -fuzz=FuzzQuery -fuzztime 30s
```

## Query Generation

The `seed-queries.test` file has a number of randomly generated queries related to the seed data metrics.

Many of these have been generated using the [https://github.com/cortexproject/promqlsmith](https://github.com/cortexproject/promqlsmith) utility.

The `demo/main.go` can be modified to not connect to a running Prometheus instance, and instead we hard code our test metrics and labels and then have the utility generate it's queries.

An example modification to the `run()` is as follows;

```go
func run() error {
	now := time.Now()
	series := []model.LabelSet{
		{
			"__name__": "http_requests",
			"job":      "api-server",
			"instance": "0",
			"group":    "production",
		},
		{
			"__name__": "http_requests",
			"job":      "api-server",
			"instance": "1",
			"group":    "production",
		},
		{
			"__name__": "http_requests",
			"job":      "api-server",
			"instance": "0",
			"group":    "canary",
		},
		{
			"__name__": "http_requests",
			"job":      "api-server",
			"instance": "1",
			"group":    "canary",
		},
		{
			"__name__": "some_metric_with_gaps",
		},
		{
			"__name__": "some_metric_all_stale",
		},
		{
			"__name__": "some_metric_often_stale",
		},
		{
			"__name__": "some_metric_with_stale_marker",
		},
		{
			"__name__": "some_metric_all_gaps",
		},
		{
			"__name__": "mixed_metric",
		},
		{
			"__name__": "mixed_metric_histogram_first",
		},
		{
			"__name__": "mixed_metric_float_first",
		},
		{
			"__name__": "metric",
			"type":     "floats",
		},
		{
			"__name__": "metric",
			"type":     "histograms",
		},
		{
			"__name__": "other_metric",
			"type":     "floats",
		},
		{
			"__name__": "other_metric",
			"type":     "histogram",
		},
		{
			"__name__": "other_metric",
			"type":     "mixed",
		},
	}

	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(getAvailableFunctions()),
		promqlsmith.WithEnabledBinOps(enabledBinops),
		promqlsmith.WithEnableVectorMatching(true),
	}
	ps := promqlsmith.New(rnd, modelLabelSetToLabels(series), opts...)

	for i := 0; i < 100; i++ {

		expr := ps.WalkInstantQuery()
		query := expr.Pretty(0)
        fmt.Printf("%s\n", strings.ReplaceAll(query, "\n", ""))
	}
	return nil
}
```
Note that this utility has a number of unsupported functions, including various `histogram_*`, and geometric functions. These sort of queries will need to be manually added to the seed queries list.