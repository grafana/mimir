# Query Fuzz Testing

This file contains notes related to the addition of Fuzz tests for the Mimir query engine.

Fuzz tests have been added to the `engine_fuzz_test.go` file. 

These will run as a normal unit test, or they can be invoked with the Go Fuzz engine with `go test -fuzz=FuzzQuery`

In either case, the test function loads a set of sample data from `seed-data.test` and then runs a range of sample queries from `seed-queries.test`.

The test assertions are that the Prometheus PromQL Engine returns the same result as the Mimir Query Engine. Considered in this test are the result errors, result values and result annotations.

There are some caveats to this comparison;

* there are cases where query result annotations (error/warning) do not match. The Mimir Query Engine has AST optimisation where a right hand side expression will not be evaluated if the left side has not results
* there are cases where query result annotations do not match on quantile percentile being out of bounds. The warning annotation includes a position in the original query of the invalid percentile and there is a difference in whitespace trimming of the query expressions between engines which results in a difference
* there are some expressions not yet supported by the Mimir Query Engine, such as info, mad_over_time, sort_by_label and sort_by_label_desc
* there are some expressions which do not have a deterministic result - ie topk(), bottomk() are undefined if there are multiple series with the same value

## Fuzz Usage

`go test -fuzz=FuzzQuery`

When invoked the seed queries are all tested first, and then the Go Fuzzer will start generating new string queries based off this seed corpus.

If a generated string fails the test assertions, it's query is written to a new file in the `testdata/fuzz` directory. 

These failures are always re-tested when the test cases are either run as unit tests or fuzz tests.

By default, the fuzz tests will continue until a failure is found. A time bound can be added to the fuzz run as follows;

`go test -fuzz=FuzzQuery -fuzztime 30s`

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