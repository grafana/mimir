// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache_test

import (
	"context"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/caching"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/splitandcache"
)

// TestUpstreamTestCases runs upstream Prometheus test cases with splitting and caching enabled.
// This is analogous to streamingpromql.TestUpstreamTestCases.
func TestUpstreamTestCases(t *testing.T) {
	dir := os.DirFS("../../../testdata")
	testFiles, err := fs.Glob(dir, "upstream/*.test")
	require.NoError(t, err)
	require.NotEmpty(t, testFiles, "expected to find test files")

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			// Note that we get the equivalent test coverage from ours/native_histograms_delayed_name_removal_disabled.test.
			if strings.Contains(testFile, "native_histograms.test") {
				t.Skip("native_histograms.test tests require delayed name removal to be enabled, but this test exercises the optimization pass with delayed name removal disabled")
			}

			runTestFile(t, dir, testFile)
		})
	}
}

// TestOurTestCases runs Mimir's test cases with splitting and caching enabled.
// This is analogous to streamingpromql.TestOurTestCases.
func TestOurTestCases(t *testing.T) {
	dir := os.DirFS("../../../testdata")
	testFiles, err := fs.Glob(dir, "ours*/*.test")
	require.NoError(t, err)
	require.NotEmpty(t, testFiles, "expected to find test files")

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			runTestFile(t, dir, testFile)
		})
	}
}

func runTestFile(t *testing.T, dir fs.FS, testFile string) {
	f, err := dir.Open(testFile)
	require.NoError(t, err)
	defer f.Close()
	b, err := io.ReadAll(f)
	require.NoError(t, err)

	opts := streamingpromql.NewTestEngineOpts()
	opts.RangeQuerySplittingAndCaching.SplitEnabled = true
	opts.RangeQuerySplittingAndCaching.SplitInterval = 5 * time.Minute
	opts.RangeQuerySplittingAndCaching.CacheEnabled = true
	opts.RangeQuerySplittingAndCaching.CacheClient = &blackholeCache{}
	opts.RangeQuerySplittingAndCaching.CacheMetrics = splitandcache.NewResultsCacheMetrics("query_range", opts.CommonOpts.Reg)
	opts.CachePrefixGenerator = caching.StaticPrefixGenerator("test")

	require.False(t, opts.CommonOpts.EnableDelayedNameRemoval, "these tests assume delayed name removal is disabled, if it is enabled by default, please remove the skip calls in this file")
	if strings.Contains(testFile, "name_label_dropping") {
		t.Skip("name_label_dropping tests require delayed name removal to be enabled, but this test exercises the optimization pass with delayed name removal disabled")
	}

	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(opts.CommonOpts.Reg), planner)
	require.NoError(t, err)

	promqltest.RunTest(t, string(b), engine)
}

type blackholeCache struct{}

func (b *blackholeCache) GetMulti(ctx context.Context, keys []string, opts ...cache.Option) map[string][]byte {
	// Nothing to do.
	return nil
}

func (b *blackholeCache) GetMultiWithError(ctx context.Context, keys []string, opts ...cache.Option) (map[string][]byte, error) {
	// Nothing to do.
	return nil, nil
}

func (b *blackholeCache) SetAsync(key string, value []byte, ttl time.Duration) {
	// Nothing to do.
}

func (b *blackholeCache) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	// Nothing to do.
}

func (b *blackholeCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Nothing to do.
	return nil
}

func (b *blackholeCache) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Nothing to do.
	return nil
}

func (b *blackholeCache) Delete(ctx context.Context, key string) error {
	// Nothing to do.
	return nil
}

func (b *blackholeCache) Stop() {
	// Nothing to do.
}

func (b *blackholeCache) Name() string {
	return "blackhole"
}
