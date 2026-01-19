// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"bufio"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	mqetest "github.com/grafana/mimir/pkg/streamingpromql/testutils"
)

const (
	// commentLinePrefix is the prefix we use in the query seeding file to indicate a comment
	commentLinePrefix = "#"
	// ignoreAnnotationDirective is the prefix we use in the query seeding to flag that we will not match on query result annotations - trailing space is intentional
	ignoreAnnotationDirective = "[ignore_annotation] "
)

// A fuzzTestEnvironment holds our Prometheus and MQE QueryEngines and a Queryable (storage).
// A fuzzTestEnvironment also holds rules for how we process certain queries.
type fuzzTestEnvironment struct {
	prometheus        promql.QueryEngine
	mqe               promql.QueryEngine
	queryable         storage.Queryable
	ignoreAnnotations map[string]bool
}

// A seedFuzzFunc add a PromQL query string to the fuzz corpus (ie t.Add(...))
// This is abstracted to allow for the implementation to determine the actual time ranges and step size used in Fuzz corpus seeding
type seedFuzzFunc func(f *testing.F, query string)

// seedQueryCorpus loads PromQL queries from a given file path and adds them to the Fuzz test corpus via the seedFuzzFunc().
// seedQueryCorpus also processes [ignore_annotation] directives in the given file and stores the ignore annotation rules in the given queryEngines.
func seedQueryCorpus(f *testing.F, queryFile string, seedFuzzFunc seedFuzzFunc, testEnvironment *fuzzTestEnvironment) {
	file, err := os.Open(queryFile)
	require.NoError(f, err)
	defer file.Close()

	ignoreAnnotations := make(map[string]bool, 500)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		query := scanner.Text()

		if len(query) == 0 || strings.HasPrefix(query, commentLinePrefix) {
			continue
		}

		query, found := strings.CutPrefix(query, ignoreAnnotationDirective)
		ignoreAnnotations[query] = found

		seedFuzzFunc(f, query)
	}

	if err := scanner.Err(); err != nil {
		require.NoError(f, err)
	}

	testEnvironment.ignoreAnnotations = ignoreAnnotations
}

// buildStorage constructs a new Queryable (TestStorage) which is seeded from the given file path.
func buildStorage(f *testing.F, dataFile string) storage.Queryable {
	data, err := os.ReadFile(dataFile)
	require.NoError(f, err)

	testStorage := promqltest.LoadedStorage(f, string(data))
	f.Cleanup(func() { require.NoError(f, testStorage.Close()) })

	return testStorage
}

// buildFuzzTestEnvironment constructs a new fuzzTestEnvironment - which encapsulates a MQE and Prometheus query engine and a Queryable (TestStorage).
// datafile is a file path used to seed the TestStorage.
// queryFile is a file path used to seed the queries Fuzz corpus.
// seedFuzzFunc is a function which actually performs the Fuzz seeding for each query string.
func buildFuzzTestEnvironment(f *testing.F, dataFile string, queryFile string, seedFuzzFunc seedFuzzFunc) *fuzzTestEnvironment {
	opts := NewTestEngineOpts()

	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(f, err)
	mqe, err := NewEngine(opts, NewStaticQueryLimitsProvider(0, false), stats.NewQueryMetrics(nil), planner)
	require.NoError(f, err)

	environment := &fuzzTestEnvironment{
		prometheus: promql.NewEngine(opts.CommonOpts),
		mqe:        mqe,
		queryable:  buildStorage(f, dataFile),
	}

	seedQueryCorpus(f, queryFile, seedFuzzFunc, environment)
	return environment
}

// fixUpAndAssertPostingErrors addresses an engine mismatch in a postings error message.
// For the relevant Prometheus error, this function will manually assert for the expected MQE error string and modify the MQE error so it will pass a later RequireEqualResults() test.
func fixUpAndAssertPostingErrors(t *testing.T, prom, mqe *promql.Result) {
	if mqe != nil && prom != nil && prom.Err != nil && prom.Err.Error() == "expanding series: unexpected all postings" {
		require.EqualError(t, mqe.Err, "unexpected all postings")
		mqe.Err = prom.Err
	}
}

// testInstantQueries executes the given query against both engines and asserts that the results match
func testInstantQueries(t *testing.T, startT time.Time, query string, testEnvironment *fuzzTestEnvironment) {
	prometheusQuery, prometheusError := testEnvironment.prometheus.NewInstantQuery(context.Background(), testEnvironment.queryable, nil, query, startT)
	mqeQuery, mqeError := testEnvironment.mqe.NewInstantQuery(context.Background(), testEnvironment.queryable, nil, query, startT)

	// if the Prometheus engine can not prepare the query then we expect the MQE to also fail
	if prometheusError != nil {
		require.Errorf(t, mqeError, "Failed to create query: Prometheus' engine returned error, but MQE returned no error.\nQuery: %q", query)
		return
	}
	require.NoError(t, mqeError, "Failed to create query: MQE returned error, but Prometheus' engine returned no error.\nQuery: %q", query)
	require.NotNilf(t, prometheusQuery, "Failed to create query: Prometheus' engine query expected not to be nil.\nQuery: %q", query)
	require.NotNilf(t, mqeQuery, "Failed to create query: MQE engine query expected not to be nil.\nQuery: %q", query)

	executeQueriesAndCompareResults(t, *testEnvironment, mqeQuery, prometheusQuery, query)
}

// testRangeQueries executes the given query against both engines and asserts that the results match
func testRangeQueries(t *testing.T, startT time.Time, endT time.Time, step time.Duration, query string, testEnvironment *fuzzTestEnvironment) {
	prometheusQuery, prometheusError := testEnvironment.prometheus.NewRangeQuery(context.Background(), testEnvironment.queryable, nil, query, startT, endT, step)
	mqeQuery, mqeError := testEnvironment.mqe.NewRangeQuery(context.Background(), testEnvironment.queryable, nil, query, startT, endT, step)

	// Note - prometheus validates time ranges and steps in http api handler, so it will not return an error at query preparation time
	// Note - max resolution size (end-start/step) is validated in codec.go so we don't validate that here either
	if prometheusError != nil || startT.After(endT) {
		require.Errorf(t, mqeError, "Failed to create query: Prometheus' engine returned error, but MQE returned no error.\nQuery: %q", query)
		return
	}
	require.NoError(t, mqeError, "Failed to create query: MQE returned error, but Prometheus' engine returned no error.\nQuery: %q", query)
	require.NotNilf(t, prometheusQuery, "Failed to create query: Prometheus' engine query expected not to be nil.\nQuery: %q", query)
	require.NotNilf(t, mqeQuery, "Failed to create query: MQE engine query expected not to be nil.\nQuery: %q", query)

	executeQueriesAndCompareResults(t, *testEnvironment, mqeQuery, prometheusQuery, query)
}

// doComparisonQuery invokes the given prepared queries against the given engines and validate that the results (inc series, errors, annotations) match.
// engines also contains rules for how to compare the results (specifically rules about which queries we ignore annotation string mis-matches).
func executeQueriesAndCompareResults(t *testing.T, testEnvironment fuzzTestEnvironment, mqeQuery promql.Query, prometheusQuery promql.Query, query string) {
	// Execute our queries
	defer prometheusQuery.Close()
	prom := prometheusQuery.Exec(context.Background())

	defer mqeQuery.Close()
	mqe := mqeQuery.Exec(context.Background())

	// Special case for posting errors which are formatted differently in Prometheus and MQE
	fixUpAndAssertPostingErrors(t, prom, mqe)

	// Assert that the results match exactly. Note that annotations comparison will be ignored if the query is in the given ignore map
	mqetest.RequireEqualResults(t, query, prom, mqe, testEnvironment.ignoreAnnotations[query])
}

// FuzzQuery is a test function which invokes a set of queries against the Prometheus and MQE query engines and validates that their responses are identical.
// FuzzQuery will run like a normal unit test when used with go test, and can be used for fuzz testing with go test -fuzz=FuzzQuery
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

	engines := buildFuzzTestEnvironment(f, "testdata/fuzz/data/seed-data.test", "testdata/fuzz/data/seed-queries.test", seedFuzzFunc)

	// Note - the Go Fuzz interface only allows for simple types to passed into a Fuzz test. We can not pass in Time or Duration variables here
	f.Fuzz(func(t *testing.T, query string, startEpoch int64, endEpoch int64, durationMilliSecs int64) {

		startT := time.UnixMilli(startEpoch)
		endT := time.UnixMilli(endEpoch)
		step := time.Duration(durationMilliSecs) * time.Millisecond

		testInstantQueries(t, startT, query, engines)
		testRangeQueries(t, startT, endT, step, query, engines)
	})
}
