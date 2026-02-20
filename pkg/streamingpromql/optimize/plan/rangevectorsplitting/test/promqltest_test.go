// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// TestQuerySplitting_UpstreamTestCases runs upstream Prometheus test cases with query splitting enabled.
// This is analogous to TestUpstreamTestCases but with query splitting.
func TestQuerySplitting_UpstreamTestCases(t *testing.T) {
	testdataFS := os.DirFS("../../../../testdata")
	testFiles, err := fs.Glob(testdataFS, "upstream/*.test")
	require.NoError(t, err)

	for _, splitInterval := range querySplittingTestSplitIntervals {
		t.Run(fmt.Sprintf("split_interval_%v", splitInterval), func(t *testing.T) {
			totalQueries := 0
			queriesWithSplit := 0

			registry := prometheus.NewRegistry()
			innerEngine, cacheBackend := createSplittingEngineWithCache(t, registry, splitInterval, true, false)

			engine := &testSplittingEngine{
				engine: innerEngine,
				orgID:  "test-user",
				onQueryExec: func(splitQueriesCount uint32) {
					totalQueries++
					if splitQueriesCount > 0 {
						queriesWithSplit++
					}
				},
			}

			for _, testFile := range testFiles {
				t.Run(testFile, func(t *testing.T) {
					f, err := testdataFS.Open(testFile)
					require.NoError(t, err)
					defer f.Close()

					b, err := io.ReadAll(f)
					require.NoError(t, err)

					testScript := skipUnsupportedTests(t, string(b), testFile)

					newStorage := func(t testing.TB) storage.Storage {
						base := promqltest.LoadedStorage(t, "")
						return &storageWithCloseCallback{
							Storage: base,
							onClose: cacheBackend.Reset,
						}
					}

					promqltest.RunTestWithStorage(t, testScript, engine, newStorage)
				})
			}

			t.Logf("Total queries executed: %d", totalQueries)
			t.Logf("Queries with splitting applied: %d", queriesWithSplit)
		})
	}
}

// TestQuerySplitting_OurTestCases runs Mimir's test cases with query splitting enabled.
// This is analogous to TestOurTestCases but with query splitting.
func TestQuerySplitting_OurTestCases(t *testing.T) {
	testdataFS := os.DirFS("../../../../testdata")
	oursTests, err := fs.Glob(testdataFS, "ours/*.test")
	require.NoError(t, err)

	oursOnlyTests, err := fs.Glob(testdataFS, "ours-only/*.test")
	require.NoError(t, err)

	allTests := append(oursTests, oursOnlyTests...)
	require.NotEmpty(t, allTests, "expected to find test files")

	for _, splitInterval := range querySplittingTestSplitIntervals {
		t.Run(fmt.Sprintf("split_interval_%v", splitInterval), func(t *testing.T) {
			totalQueries := 0
			queriesWithSplit := 0

			registry := prometheus.NewRegistry()
			innerEngine, cacheBackend := createSplittingEngineWithCache(t, registry, splitInterval, false, true)

			registryDelayed := prometheus.NewRegistry()
			innerEngineDelayed, cacheBackendDelayed := createSplittingEngineWithCache(t, registryDelayed, splitInterval, true, true)

			engine := &testSplittingEngine{
				engine: innerEngine,
				orgID:  "test-user",
				onQueryExec: func(splitQueriesCount uint32) {
					totalQueries++
					if splitQueriesCount > 0 {
						queriesWithSplit++
					}
				},
			}

			engineDelayed := &testSplittingEngine{
				engine: innerEngineDelayed,
				orgID:  "test-user",
				onQueryExec: func(splitQueriesCount uint32) {
					totalQueries++
					if splitQueriesCount > 0 {
						queriesWithSplit++
					}
				},
			}

			for _, testFile := range allTests {
				t.Run(testFile, func(t *testing.T) {
					f, err := testdataFS.Open(testFile)
					require.NoError(t, err)
					defer f.Close()

					b, err := io.ReadAll(f)
					require.NoError(t, err)

					testScript := skipUnsupportedTests(t, string(b), testFile)

					// Switch to delayed name removal engine if the test file requires it
					enableDelayedNameRemoval := strings.Contains(testFile, "name_label_dropping") || strings.Contains(testFile, "delayed_name_removal_enabled")
					selectedEngine := engine
					selectedCache := cacheBackend
					if enableDelayedNameRemoval {
						selectedEngine = engineDelayed
						selectedCache = cacheBackendDelayed
					}

					newStorage := func(t testing.TB) storage.Storage {
						base := promqltest.LoadedStorage(t, "")
						return &storageWithCloseCallback{
							Storage: base,
							onClose: selectedCache.Reset,
						}
					}

					promqltest.RunTestWithStorage(t, testScript, selectedEngine, newStorage)
				})
			}

			t.Logf("Total queries executed: %d", totalQueries)
			t.Logf("Queries with splitting applied: %d", queriesWithSplit)
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
