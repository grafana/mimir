// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/tree/main/promql/engine_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package streamingpromql

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
)

// A construct to hold our query engines and storage. Also holds rules for how we process certain queries
type TestQueryContext struct {
	prometheusEngine  *promql.Engine
	mimirEngine       *Engine
	testStorage       *teststorage.TestStorage
	ignoreAnnotations map[string]bool
}

// A construct to hold our input params to each test
type TestQueryInput struct {
	// promQL query
	query string
	// range query start or instant query time
	startEpoch int64
	// range query end time
	endEpoch int64
	// range query step size
	durationMilliSecs int64
}

// A construct to hold the result of a query preparation
type EngineQueryPreparationResult struct {
	query promql.Query
	error error
}

// A construct to hold the query preparation results for both engines
type MultiEnginePrepareResult struct {
	prom           EngineQueryPreparationResult
	mimir          EngineQueryPreparationResult
	isInstantQuery func() bool
}

// A utility to abstract the actual query preparation steps
type InvokeQueryEnginePreparation func(input TestQueryInput) MultiEnginePrepareResult

// A function to add a query to the Fuzz corpus (ie t.Add(...))
// This is abstracted to allow for custom corpus seeding of time ranges and step size
type SeedFuzzFunc func(t *testing.F, query string)

// Seed queries from the given file into the Fuzz test corpus
// Stores a map in the testContext which details queries which should have annotation comparison disabled
func seedQueryCorpus(f *testing.F, queryFile string, seedFuzzFunc SeedFuzzFunc, testContext *TestQueryContext) {
	queries, err := os.ReadFile(queryFile)
	require.NoError(f, err)

	ignoreAnnotations := make(map[string]bool, 500)

	for _, query := range strings.Split(string(queries), "\n") {
		if len(query) == 0 || strings.HasPrefix(query, "#") {
			continue
		}

		query, found := strings.CutPrefix(query, "[ignore_annotations] ")
		ignoreAnnotations[query] = found

		seedFuzzFunc(f, query)
	}

	testContext.ignoreAnnotations = ignoreAnnotations
}

// Build a TestStorage seeded with the data in the given file
func buildStorage(f *testing.F, datafile string) *teststorage.TestStorage {
	data, err := os.ReadFile(datafile)
	require.NoError(f, err)

	testStorage := promqltest.LoadedStorage(f, string(data))
	f.Cleanup(func() { require.NoError(f, testStorage.Close()) })

	return testStorage
}

// Build a new TestQueryContext
// This includes our Mimir & Prometheus engines, and a seeded TestStorage
// The seed queries are also loaded into the Fuzz corpus
// A seeding function is passed in defining how we will seed the Fuzz corpus
func buildQueryContext(f *testing.F, datafile string, queryFile string, seedFuzzFunc SeedFuzzFunc) *TestQueryContext {
	opts := NewTestEngineOpts()

	mimirEngine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), NewQueryPlanner(opts), log.NewNopLogger())
	require.NoError(f, err)

	prometheusEngine := promql.NewEngine(opts.CommonOpts)

	testContext := TestQueryContext{
		prometheusEngine: prometheusEngine,
		mimirEngine:      mimirEngine,
		testStorage:      buildStorage(f, datafile),
	}

	seedQueryCorpus(f, queryFile, seedFuzzFunc, &testContext)
	return &testContext
}

// There is a mismatch in a postings error message.
// We manually assert on the expected Mimir error string and modify the mimir error so it will pass the RequireEqualResults()
func fixUpAndAssertPostingErrors(t *testing.T, prom, mimir *promql.Result) {
	if mimir != nil && prom != nil && prom.Err != nil && prom.Err.Error() == "expanding series: unexpected all postings" {
		assert.Equal(t, "unexpected all postings", mimir.Err.Error())
		mimir.Err = prom.Err
	}
}

func buildInstantQueryPreparationFunc(ctx *TestQueryContext) InvokeQueryEnginePreparation {
	return func(input TestQueryInput) MultiEnginePrepareResult {

		res := MultiEnginePrepareResult{
			prom:           EngineQueryPreparationResult{},
			mimir:          EngineQueryPreparationResult{},
			isInstantQuery: func() bool { return true },
		}

		start := time.UnixMilli(input.startEpoch)
		res.prom.query, res.prom.error = ctx.prometheusEngine.NewInstantQuery(context.Background(), ctx.testStorage, nil, input.query, start)
		res.mimir.query, res.mimir.error = ctx.mimirEngine.NewInstantQuery(context.Background(), ctx.testStorage, nil, input.query, start)

		return res
	}
}

func buildRangeQueryPreparationFunc(ctx *TestQueryContext) InvokeQueryEnginePreparation {
	return func(input TestQueryInput) MultiEnginePrepareResult {

		res := MultiEnginePrepareResult{
			prom:           EngineQueryPreparationResult{},
			mimir:          EngineQueryPreparationResult{},
			isInstantQuery: func() bool { return false },
		}

		start := time.UnixMilli(input.startEpoch)
		end := time.UnixMilli(input.endEpoch)
		step := time.Duration(input.durationMilliSecs) * time.Millisecond

		res.prom.query, res.prom.error = ctx.prometheusEngine.NewRangeQuery(context.Background(), ctx.testStorage, nil, input.query, start, end, step)
		res.mimir.query, res.mimir.error = ctx.mimirEngine.NewRangeQuery(context.Background(), ctx.testStorage, nil, input.query, start, end, step)

		return res
	}
}

// Run the given query against Prometheus and Mimir query engines and validate that the results (inc series, errors, annotations) match
func doComparisonQuery(t *testing.T, input TestQueryInput, ctx TestQueryContext, prepareQueriesFunc InvokeQueryEnginePreparation) {

	prep := prepareQueriesFunc(input)

	// Note - prometheus validates time ranges and steps in http api handler, so it will not return an error at query preparation time
	// Note - max resolution size (end-start/step) is validated in codec.go so we don't validate that here
	if prep.prom.error != nil || (!prep.isInstantQuery() && input.startEpoch > input.endEpoch) {
		require.NotNil(t, prep.mimir.error)
		return
	}

	var prom, mimir *promql.Result

	// Execute our queries
	defer prep.prom.query.Close()
	prom = prep.prom.query.Exec(context.Background())

	defer prep.mimir.query.Close()
	mimir = prep.mimir.query.Exec(context.Background())

	// Replace series Metric which are nil with an empty Labels{}
	FixUpEmptyLabels(prom)

	// Special case for posting errors which are formatted differently in Prometheus and Mimir
	fixUpAndAssertPostingErrors(t, prom, mimir)

	// Assert that the results match exactly. Note that annotations comparison will be ignored if the query is in the given ignore map
	testutils.RequireEqualResults(t, input.query, prom, mimir, ctx.ignoreAnnotations[input.query])
}

// This test function will run like a normal unit test with go test
// It can be used for Fuzz testing with go test -fuzz=FuzzQuery
// These tests can be extended in 3 ways;
// a. Add more sample data to the seed-data.test file
// b. Add more sample queries to the seed-queries.test file
// c. Add additional step sizes or time ranges to consider
// When the Go Fuzzer is used, it will randomize the inputs of query string, start time, end time and step size.
func FuzzQuery(f *testing.F) {

	steps := []time.Duration{
		time.Second,
		time.Second * 2,
		time.Second * 10,
		time.Second * 30,
		time.Second * 59,
		time.Minute,
		time.Second * 61,
		time.Minute * 10,
		time.Hour * 24 * 365 * 100,
	}

	epoch := time.Unix(0, 0)

	ranges := [][]time.Time{
		// invalid time range, as start > end
		{epoch.Add(time.Second), epoch},

		// start == end
		{epoch, epoch},

		// explore below our sample interval
		{epoch, epoch.Add(time.Second * 1)},

		// explore the bounds around our sample interval
		{epoch, epoch.Add(time.Second * 59)},
		{epoch, epoch.Add(time.Second * 60)},
		{epoch, epoch.Add(time.Second * 61)},
		{epoch.Add(time.Second * 4), epoch.Add(time.Second * 61)},

		// explore the bounds after we have multiple samples
		{epoch, epoch.Add(time.Minute*5 - time.Second)},
		{epoch, epoch.Add(time.Minute * 5)},
		{epoch, epoch.Add(time.Minute*5 + time.Second)},
		{epoch.Add(time.Second * 4), epoch.Add(time.Second * 61)},
		{epoch.Add(time.Minute * 2), epoch.Add(time.Second * 61)},
	}

	// For each query string we will test for each range & step combination
	// The fuzzer will then be able then randomise on the query, time range and/or step size
	seedFuzzFunc := func(t *testing.F, query string) {
		for _, step := range steps {
			for _, rang := range ranges {
				t.Add(query, rang[0].UnixMilli(), rang[1].UnixMilli(), step.Milliseconds())
			}
		}
	}

	testCtx := buildQueryContext(f, "testdata/fuzz/data/seed-data.test", "testdata/fuzz/data/seed-queries.test", seedFuzzFunc)

	instantQueryPreparationHandler := buildInstantQueryPreparationFunc(testCtx)
	rangeQueryPreparationHandler := buildRangeQueryPreparationFunc(testCtx)

	f.Fuzz(func(t *testing.T, query string, startEpoch int64, endEpoch int64, durationMilliSecs int64) {
		input := TestQueryInput{
			query:             query,
			startEpoch:        startEpoch,
			endEpoch:          endEpoch,
			durationMilliSecs: durationMilliSecs,
		}
		doComparisonQuery(t, input, *testCtx, instantQueryPreparationHandler)
		doComparisonQuery(t, input, *testCtx, rangeQueryPreparationHandler)
	})
}
