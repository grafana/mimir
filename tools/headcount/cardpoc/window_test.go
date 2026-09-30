// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"os"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/headcount/cardgen"
)

func TestNameCountsForWindow_SumOvercountsDedupExact(t *testing.T) {
	pop := nameCountsTestPopulation(t) // 3 hours, churn every hour
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err := cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 1, Seed: 1})
	require.NoError(t, err)

	cfg := pop.Config()
	start, end := cfg.Start.UnixMilli(), cfg.End.UnixMilli()
	w, err := NameCountsForWindow(bucket, start, end)
	require.NoError(t, err)
	require.Len(t, w.Ranges, 3)

	summedTotal, exactTotal := 0, 0
	for name, exact := range w.Exact {
		m := labels.MustNewMatcher(labels.MatchEqual, "__name__", name)
		require.Equal(t, pop.Truth([]*labels.Matcher{m}, start, end), exact, name)
		require.GreaterOrEqual(t, w.Summed[name], exact, name)
		summedTotal += w.Summed[name]
		exactTotal += exact
	}
	require.Equal(t, pop.Truth(nil, start, end), exactTotal)
	require.Greater(t, summedTotal, exactTotal, "series living in more than one hour must be counted more than once by the sum")
	require.Positive(t, w.IndexBytes)

	hour := time.Hour.Milliseconds()
	_, err = NameCountsForWindow(bucket, start+hour/2, end)
	require.ErrorContains(t, err, "cuts through")
	_, err = NameCountsForWindow(bucket, start, end+hour)
	require.ErrorContains(t, err, "before the window's end")
}
