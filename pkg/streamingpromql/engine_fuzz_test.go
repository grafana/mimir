// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"bufio"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
)

// A queryEngines holds our Prometheus and Mimir QueryEngines and a Queryable (storage).
// A queryEngines also holds rules for how we process certain queries.
type queryEngines struct {
	prometheus        promql.QueryEngine
	mimir             promql.QueryEngine
	testStorage       storage.Queryable
	ignoreAnnotations map[string]bool
}

// A fuzzingArg holds our input params to each test - including the PromQL string, query time ranges and step size.
// Note - end and step are ignored for range queries.
type fuzzingArg struct {
	// promQL query
	query string
	// range query start or instant query time
	start time.Time
	// range query end time
	end time.Time
	// range query step size
	step time.Duration
}

// A engineQueryPreparationResult holds a query engine's result of a query preparation.
type engineQueryPreparationResult struct {
	query promql.Query
	error error
}

// A multiEnginePrepareResult holds the combined engineQueryPreparationResults for both engines.
type multiEnginePrepareResult struct {
	prometheus     engineQueryPreparationResult
	mimir          engineQueryPreparationResult
	isInstantQuery func() bool
}

// A invokeQueryEnginePreparation takes a fuzzingArg and prepares the query on each query engine
// The query preparation result or error is returned within in a multiEnginePrepareResult.
type invokeQueryEnginePreparation func(input fuzzingArg, engines queryEngines) multiEnginePrepareResult

// A seedFuzzFunc add a PromQL query string to the Fuzz corpus (ie t.Add(...))
// This is abstracted to allow for the implementation to determine the actual time ranges and step size used in Fuzz corpus seeding
type seedFuzzFunc func(f *testing.F, query string)

// seedQueryCorpus loads PromQL queries from a given file path and adds them to the Fuzz test corpus via the seedFuzzFunc().
// seedQueryCorpus also processes [ignore_annotation] directives in the given file and stores the ignore annotation rules in the given queryEngines.
func seedQueryCorpus(f *testing.F, queryFile string, seedFuzzFunc seedFuzzFunc, testEngines *queryEngines) {
	file, err := os.Open(queryFile)
	require.NoError(f, err)
	defer file.Close()

	ignoreAnnotations := make(map[string]bool, 500)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		query := scanner.Text()

		if len(query) == 0 || strings.HasPrefix(query, "#") {
			continue
		}

		query, found := strings.CutPrefix(query, "[ignore_annotations] ")
		ignoreAnnotations[query] = found

		seedFuzzFunc(f, query)
	}

	if err := scanner.Err(); err != nil {
		require.NoError(f, err)
	}

	testEngines.ignoreAnnotations = ignoreAnnotations
}

// buildStorage constructs a new Queryable (TestStorage) which is seeded from the given file path.
func buildStorage(f *testing.F, dataFile string) storage.Queryable {
	data, err := os.ReadFile(dataFile)
	require.NoError(f, err)

	testStorage := promqltest.LoadedStorage(f, string(data))
	f.Cleanup(func() { require.NoError(f, testStorage.Close()) })

	return testStorage
}

// buildQueryEngines constructs a new queryEngines - which encapsulates a Mimir and Prometheus query engine and a Queryable (TestStorage).
// datafile is a file path used to seed the TestStorage.
// queryFile is a file path used to seed the queries Fuzz corpus.
// seedFuzzFunc is a function which actually performs the Fuzz seeding for each query string.
func buildQueryEngines(f *testing.F, dataFile string, queryFile string, seedFuzzFunc seedFuzzFunc) *queryEngines {
	opts := NewTestEngineOpts()

	mimir, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), NewQueryPlanner(opts), log.NewNopLogger())
	require.NoError(f, err)

	engines := queryEngines{
		prometheus:  promql.NewEngine(opts.CommonOpts),
		mimir:       mimir,
		testStorage: buildStorage(f, dataFile),
	}

	seedQueryCorpus(f, queryFile, seedFuzzFunc, &engines)
	return &engines
}

// fixUpAndAssertPostingErrors addresses an engine mismatch in a postings error message.
// For the relevant Prometheus error, this function will manually assert for the expected Mimir error string and modify the Mimir error so it will pass a later RequireEqualResults() test.
func fixUpAndAssertPostingErrors(t *testing.T, prom, mimir *promql.Result) {
	if mimir != nil && prom != nil && prom.Err != nil && prom.Err.Error() == "expanding series: unexpected all postings" {
		assert.Equal(t, "unexpected all postings", mimir.Err.Error())
		mimir.Err = prom.Err
	}
}

// buildInstantQueryPreparationFunc constructs a invokeQueryEnginePreparation()
// The returned function can be invoked to prepare an instant PromQL query against the given query engines.
func buildInstantQueryPreparationFunc() invokeQueryEnginePreparation {
	return func(input fuzzingArg, engines queryEngines) multiEnginePrepareResult {

		res := multiEnginePrepareResult{
			prometheus:     engineQueryPreparationResult{},
			mimir:          engineQueryPreparationResult{},
			isInstantQuery: func() bool { return true },
		}

		res.prometheus.query, res.prometheus.error = engines.prometheus.NewInstantQuery(context.Background(), engines.testStorage, nil, input.query, input.start)
		res.mimir.query, res.mimir.error = engines.mimir.NewInstantQuery(context.Background(), engines.testStorage, nil, input.query, input.start)

		return res
	}
}

// buildRangeQueryPreparationFunc constructs a invokeQueryEnginePreparation()
// The returned function can be invoked to prepare a range PromQL query against the given query engines.
func buildRangeQueryPreparationFunc() invokeQueryEnginePreparation {
	return func(input fuzzingArg, engines queryEngines) multiEnginePrepareResult {

		res := multiEnginePrepareResult{
			prometheus:     engineQueryPreparationResult{},
			mimir:          engineQueryPreparationResult{},
			isInstantQuery: func() bool { return false },
		}

		res.prometheus.query, res.prometheus.error = engines.prometheus.NewRangeQuery(context.Background(), engines.testStorage, nil, input.query, input.start, input.end, input.step)
		res.mimir.query, res.mimir.error = engines.mimir.NewRangeQuery(context.Background(), engines.testStorage, nil, input.query, input.start, input.end, input.step)

		return res
	}
}

// doComparisonQuery invokes the given fuzzingArg (query) query against the given engines and validate that the results (inc series, errors, annotations) match.
// input contains the metadata about the query such as the query string and time range.
// engines also contains rules for how to compare the results (specifically rules about which queries we ignore annotation string mis-matches).
// prepareQueries is an abstraction around how queries are prepared against the query engines.
func doComparisonQuery(t *testing.T, input fuzzingArg, engines queryEngines, prepareQueries invokeQueryEnginePreparation) {

	prep := prepareQueries(input, engines)

	// Note - prometheus validates time ranges and steps in http api handler, so it will not return an error at query preparation time
	// Note - max resolution size (end-start/step) is validated in codec.go so we don't validate that here
	if prep.prometheus.error != nil || (!prep.isInstantQuery() && input.start.After(input.end)) {
		require.NotNil(t, prep.mimir.error)
		return
	}

	var prom, mimir *promql.Result

	// Execute our queries
	defer prep.prometheus.query.Close()
	prom = prep.prometheus.query.Exec(context.Background())

	defer prep.mimir.query.Close()
	mimir = prep.mimir.query.Exec(context.Background())

	// Replace series Metric which are nil with an empty Labels{}
	err := FixUpEmptyLabels(prom)
	require.NoError(t, err)

	// Special case for posting errors which are formatted differently in Prometheus and Mimir
	fixUpAndAssertPostingErrors(t, prom, mimir)

	// Assert that the results match exactly. Note that annotations comparison will be ignored if the query is in the given ignore map
	testutils.RequireEqualResults(t, input.query, prom, mimir, engines.ignoreAnnotations[input.query])
}

// FuzzQuery is a test function which invokes a set of queries against the Prometheus and Mimir query engines and validates that their responses are identical.
// FuzzQuery will run like a normal unit test when used with go test, and can be used for Fuzz testing with go test -fuzz=FuzzQuery
// These tests can be extended in 3 ways;
// * Add more sample data to the seed-data.test file
// * Add more sample queries to the seed-queries.test file
// * Add additional step sizes or time ranges to consider
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

	// Generate Fuzzing arguments with different ranges and steps combination for the passed query
	seedFuzzFunc := func(f *testing.F, query string) {
		for _, step := range steps {
			for _, rang := range ranges {
				f.Add(query, rang[0].UnixMilli(), rang[1].UnixMilli(), step.Milliseconds())
			}
		}
	}

	engines := buildQueryEngines(f, "testdata/fuzz/data/seed-data.test", "testdata/fuzz/data/seed-queries.test", seedFuzzFunc)

	instantQueryPreparationHandler := buildInstantQueryPreparationFunc()
	rangeQueryPreparationHandler := buildRangeQueryPreparationFunc()

	f.Fuzz(func(t *testing.T, query string, startEpoch int64, endEpoch int64, durationMilliSecs int64) {
		input := fuzzingArg{
			query: query,
			start: time.UnixMilli(startEpoch),
			end:   time.UnixMilli(endEpoch),
			step:  time.Duration(durationMilliSecs) * time.Millisecond,
		}
		doComparisonQuery(t, input, *engines, instantQueryPreparationHandler)
		doComparisonQuery(t, input, *engines, rangeQueryPreparationHandler)
	})
}
