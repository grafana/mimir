// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
)

var querySplittingTestSplitIntervals = []time.Duration{
	10 * time.Second,
	5 * time.Minute,
	2 * time.Hour,
}

// TestQuerySplitting_UpstreamTestCases runs upstream Prometheus test cases with query splitting enabled.
// This is analogous to TestUpstreamTestCases but with query splitting.
func TestQuerySplitting_UpstreamTestCases(t *testing.T) {
	testdataFS := os.DirFS("../../../../testdata")
	testFiles, err := fs.Glob(testdataFS, "upstream/*.test")
	require.NoError(t, err)

	for _, splitInterval := range querySplittingTestSplitIntervals {
		t.Run(fmt.Sprintf("split_interval_%v", splitInterval), func(t *testing.T) {
			t.Parallel()
			registry := prometheus.NewRegistry()
			innerEngine, cacheBackend := createSplittingEngineWithCache(t, registry, splitInterval, true, false)
			engine := &testSplittingEngine{engine: innerEngine, orgID: "test-user"}

			for _, testFile := range testFiles {
				t.Run(testFile, func(t *testing.T) {
					runTestFile(t, testdataFS, testFile, engine, cacheBackend.Reset)
				})
			}

			t.Logf("Total queries executed: %d", engine.TotalQueries())
			t.Logf("Queries with splitting applied: %d", engine.QueriesWithSplit())
		})
	}
}

// TestQuerySplitting_OurTestCases runs Mimir's test cases with query splitting enabled.
// This is analogous to TestOurTestCases but with query splitting.
func TestQuerySplitting_OurTestCases(t *testing.T) {
	testdataFS := os.DirFS("../../../../testdata")
	oursTests, err := fs.Glob(testdataFS, "ours/*.test")
	require.NoError(t, err)
	require.NotEmpty(t, oursTests, "expected to find test files")

	oursOnlyTests, err := fs.Glob(testdataFS, "ours-only/*.test")
	require.NoError(t, err)
	require.NotEmpty(t, oursOnlyTests, "expected to find test files")

	allTests := append(oursTests, oursOnlyTests...)

	for _, splitInterval := range querySplittingTestSplitIntervals {
		t.Run(fmt.Sprintf("split_interval_%v", splitInterval), func(t *testing.T) {
			t.Parallel()
			registry := prometheus.NewRegistry()
			innerEngine, cacheBackend := createSplittingEngineWithCache(t, registry, splitInterval, false, true)
			registryDelayed := prometheus.NewRegistry()
			innerEngineDelayed, cacheBackendDelayed := createSplittingEngineWithCache(t, registryDelayed, splitInterval, true, true)
			engine := &testSplittingEngine{engine: innerEngine, orgID: "test-user"}
			engineDelayed := &testSplittingEngine{engine: innerEngineDelayed, orgID: "test-user"}

			for _, testFile := range allTests {
				t.Run(testFile, func(t *testing.T) {
					enableDelayedNameRemoval := strings.Contains(testFile, "name_label_dropping") || strings.Contains(testFile, "delayed_name_removal_enabled")
					selectedEngine := engine
					selectedCache := cacheBackend
					if enableDelayedNameRemoval {
						selectedEngine = engineDelayed
						selectedCache = cacheBackendDelayed
					}
					runTestFile(t, testdataFS, testFile, selectedEngine, selectedCache.Reset)
				})
			}

			t.Logf("Total queries executed: %d", engine.TotalQueries()+engineDelayed.TotalQueries())
			t.Logf("Queries with splitting applied: %d", engine.QueriesWithSplit()+engineDelayed.QueriesWithSplit())
		})
	}
}

// TestQuerySplitting_RangeVectorSplittingTestCases runs the .test files in this package directory (range-vector-splitting-specific cases).
func TestQuerySplitting_RangeVectorSplittingTestCases(t *testing.T) {
	packageFS := os.DirFS(".")
	testFiles, err := fs.Glob(packageFS, "*.test")
	require.NoError(t, err)
	require.NotEmpty(t, testFiles, "expected to find .test files in package directory")

	for _, splitInterval := range querySplittingTestSplitIntervals {
		t.Run(fmt.Sprintf("split_interval_%v", splitInterval), func(t *testing.T) {
			t.Parallel()
			registry := prometheus.NewRegistry()
			innerEngine, cacheBackend := createSplittingEngineWithCache(t, registry, splitInterval, false, true)
			engine := &testSplittingEngine{engine: innerEngine, orgID: "test-user"}

			for _, testFile := range testFiles {
				t.Run(testFile, func(t *testing.T) {
					runTestFile(t, packageFS, testFile, engine, cacheBackend.Reset)
				})
			}

			t.Logf("Total queries executed: %d", engine.TotalQueries())
			t.Logf("Queries with splitting applied: %d", engine.QueriesWithSplit())
		})
	}
}

// skipUnsupportedTests comments out test cases where the split implementation diverges from the Prometheus/non-split MQE implementations.
func skipUnsupportedTests(t *testing.T, testContent string, testFile string) string {
	var testCasesToSkip []string

	switch testFile {
	case "upstream/native_histograms.test":
		// The split sum_over_time sometimes cannot detect conflicting counter reset warnings.
		// See comments for rangevectorsplitting.SplitSumOverTime.
		testCasesToSkip = []string{
			`eval instant at 14m histogram_count(sum_over_time(mixed[10m]))
  expect warn msg:PromQL warning: conflicting counter resets during histogram aggregation
  expect no_info
  {} 93`,

			`eval instant at 11m histogram_count(sum_over_time(mixed[2m]))
  expect warn msg:PromQL warning: conflicting counter resets during histogram aggregation
  expect no_info
  {} 21`,

			`eval instant at 5m histogram_count(sum_over_time(reset{timing="late"}[5m]))
    expect warn msg: PromQL warning: conflicting counter resets during histogram aggregation
    {timing="late"} 7`,
		}

	default:
		return testContent
	}

	modified := testContent
	for i, testCase := range testCasesToSkip {
		if !strings.Contains(modified, testCase) {
			require.FailNow(t, "Failed to find expected test case in "+testFile,
				"Could not find test case at index %d. The test file may have changed.\nLooking for:\n%s", i, testCase)
		}

		lines := strings.Split(testCase, "\n")
		for j, line := range lines {
			lines[j] = "# SKIPPED FOR QUERY SPLITTING: " + line
		}
		commented := strings.Join(lines, "\n")

		modified = strings.Replace(modified, testCase, commented, 1)
	}

	return modified
}

// TestQuerySplitting_EvalsRunTwice verifies that each eval in the range vector splitting test files appears at least
// twice before the next clear, ensuring both cache miss and cache hit paths are tested.
func TestQuerySplitting_EvalsRunTwice(t *testing.T) {
	packageFS := os.DirFS(".")
	testFiles, err := fs.Glob(packageFS, "*.test")
	require.NoError(t, err)
	require.NotEmpty(t, testFiles)

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			f, err := packageFS.Open(testFile)
			require.NoError(t, err)
			defer f.Close()

			b, err := io.ReadAll(f)
			require.NoError(t, err)

			lines := strings.Split(string(b), "\n")

			// Between each load/clear, verify each eval appears at least twice (to test both cache miss and hit).
			evalCounts := map[string]int{}
			checkAndClear := func(lineNum int) {
				for eval, count := range evalCounts {
					require.GreaterOrEqualf(t, count, 2,
						"eval appears only %d time(s) before line %d, expected at least 2:\n%s",
						count, lineNum, eval)
				}
				clear(evalCounts)
			}
			for i, line := range lines {

				if strings.HasPrefix(line, "load ") || line == "clear" {
					checkAndClear(i + 1)
				}

				if strings.HasPrefix(line, "eval ") {
					evalCounts[line]++
				}
			}
			checkAndClear(len(lines))
		})
	}
}

func runTestFile(t *testing.T, fsys fs.FS, testFile string, engine promql.QueryEngine, cacheReset func()) {
	t.Helper()
	f, err := fsys.Open(testFile)
	require.NoError(t, err)
	defer f.Close()
	b, err := io.ReadAll(f)
	require.NoError(t, err)
	testScript := skipUnsupportedTests(t, string(b), testFile)
	newStorage := func(t testing.TB) storage.Storage {
		base := promqltest.LoadedStorage(t, "")
		return &storageWithCloseCallback{Storage: base, onClose: cacheReset}
	}
	promqltest.RunTestWithStorage(t, testScript, engine, newStorage)
}

type storageWithCloseCallback struct {
	storage.Storage
	onClose func()
}

func (s *storageWithCloseCallback) Close() error {
	if s.onClose != nil {
		s.onClose()
	}
	return s.Storage.Close()
}

type testSplittingEngine struct {
	engine           promql.QueryEngine
	orgID            string
	totalQueries     int
	queriesWithSplit int
}

// TotalQueries returns the number of queries executed through this engine.
func (e *testSplittingEngine) TotalQueries() int { return e.totalQueries }

// QueriesWithSplit returns the number of those queries that had range vector splitting applied.
func (e *testSplittingEngine) QueriesWithSplit() int { return e.queriesWithSplit }

func (e *testSplittingEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	ctx = user.InjectOrgID(ctx, e.orgID)
	query, err := e.engine.NewInstantQuery(ctx, q, opts, qs, ts)
	if err != nil {
		return nil, err
	}
	return &testSplittingQuery{Query: query, orgID: e.orgID, engine: e}, nil
}

func (e *testSplittingEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	ctx = user.InjectOrgID(ctx, e.orgID)
	query, err := e.engine.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
	if err != nil {
		return nil, err
	}
	return &testSplittingQuery{Query: query, orgID: e.orgID, engine: e}, nil
}

type testSplittingQuery struct {
	promql.Query
	orgID  string
	engine *testSplittingEngine
}

func (q *testSplittingQuery) Exec(ctx context.Context) *promql.Result {
	ctx = user.InjectOrgID(ctx, q.orgID)
	queryStats, ctx := stats.ContextWithEmptyStats(ctx)
	result := q.Query.Exec(ctx)
	q.engine.totalQueries++
	if queryStats.LoadSplitRangeVectors() > 0 {
		q.engine.queriesWithSplit++
	}
	return result
}
