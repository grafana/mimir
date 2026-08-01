// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestPartitionTSDB_SharedPostingsCacheIsIsolatedAndInvalidated(t *testing.T) {
	cfg := newTestConfig(t, false, 0)
	cfg.BlocksStorage.TSDB.SharedPostingsForMatchersCache = true
	cfg.BlocksStorage.TSDB.HeadPostingsForMatchersCacheInvalidation = true
	cfg.BlocksStorage.TSDB.HeadPostingsForMatchersCacheForce = true
	cfg.BlocksStorage.TSDB.HeadPostingsForMatchersCacheTTL = time.Hour

	reg := prometheus.NewPedanticRegistry()
	limits := validation.NewOverrides(validation.Limits{}, nil)
	r, err := New(cfg, limits, nil, log.NewNopLogger(), reg)
	require.NoError(t, err)

	open := func(partitionID int32, epoch int) *partitionTSDB {
		t.Helper()
		db, err := openPartitionTSDB(
			"tenant",
			partitionID,
			epoch,
			cfg.DataDir,
			cfg.BlocksStorage.TSDB,
			cfg.LocalBlockRetention,
			limits,
			0,
			r.seriesHashCache,
			r.headPostingsForMatchersCacheFactory,
			r.blockPostingsForMatchersCacheFactory,
			prometheus.NewRegistry(),
			log.NewNopLogger(),
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}

	partitionOne := open(1, 0)
	partitionTwo := open(2, 3)
	partitionOneNextEpoch := open(1, 4)

	appendSeries(t, partitionOne,
		labels.FromStrings(labels.MetricName, "target", "instance", "one-a"),
		labels.FromStrings(labels.MetricName, "target", "instance", "one-b"),
	)
	// Shift the target series reference in partition two so reusing partition
	// one's cached postings would return a visibly wrong series.
	appendSeries(t, partitionTwo,
		labels.FromStrings(labels.MetricName, "other", "instance", "two-other"),
		labels.FromStrings(labels.MetricName, "target", "instance", "two"),
	)
	appendSeries(t, partitionOneNextEpoch,
		labels.FromStrings(labels.MetricName, "other", "instance", "one-next-other"),
		labels.FromStrings(labels.MetricName, "target", "instance", "one-next"),
	)

	target := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "target")
	assert.Equal(t, []string{
		`{__name__="target", instance="one-a"}`,
		`{__name__="target", instance="one-b"}`,
	}, selectSeries(t, partitionOne, target))
	// Repeat the lookup to prove this test is exercising a retained cache entry.
	assert.Len(t, selectSeries(t, partitionOne, target), 2)
	assert.Equal(t, []string{
		`{__name__="target", instance="two"}`,
	}, selectSeries(t, partitionTwo, target))
	assert.Equal(t, []string{
		`{__name__="target", instance="two"}`,
	}, selectChunkSeries(t, partitionTwo, target))
	assert.Equal(t, []string{
		`{__name__="target", instance="one-next"}`,
	}, selectSeries(t, partitionOneNextEpoch, target))

	appendSeries(t, partitionOne,
		labels.FromStrings(labels.MetricName, "target", "instance", "one-c"),
	)
	assert.Equal(t, []string{
		`{__name__="target", instance="one-a"}`,
		`{__name__="target", instance="one-b"}`,
		`{__name__="target", instance="one-c"}`,
	}, selectSeries(t, partitionOne, target))

	assert.Greater(t, gatheredMetricValue(t, reg, "cortex_readcache_tsdb_head_postings_for_matchers_cache_hits_total"), float64(0))
	assert.Greater(t, gatheredMetricValue(t, reg, "cortex_readcache_tsdb_head_postings_for_matchers_cache_invalidations_total"), float64(0))
}

func appendSeries(t *testing.T, db *partitionTSDB, series ...labels.Labels) {
	t.Helper()
	app := db.Appender(t.Context())
	for i, metric := range series {
		_, err := app.Append(0, metric, int64(i), float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
}

func selectSeries(t *testing.T, db *partitionTSDB, matchers ...*labels.Matcher) []string {
	t.Helper()
	q, err := db.Querier(0, 10)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()

	set := q.Select(t.Context(), true, &storage.SelectHints{Start: 0, End: 10}, matchers...)
	var got []string
	for set.Next() {
		got = append(got, set.At().Labels().String())
	}
	require.NoError(t, set.Err())
	sort.Strings(got)
	return got
}

func selectChunkSeries(t *testing.T, db *partitionTSDB, matchers ...*labels.Matcher) []string {
	t.Helper()
	q, err := db.ChunkQuerier(0, 10)
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()

	set := q.Select(t.Context(), true, &storage.SelectHints{Start: 0, End: 10}, matchers...)
	var got []string
	for set.Next() {
		got = append(got, set.At().Labels().String())
	}
	require.NoError(t, set.Err())
	sort.Strings(got)
	return got
}

func gatheredMetricValue(t *testing.T, gatherer prometheus.Gatherer, name string) float64 {
	t.Helper()
	families, err := gatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		require.Len(t, family.Metric, 1)
		return family.Metric[0].GetCounter().GetValue()
	}
	t.Fatalf("metric %q was not gathered", name)
	return 0
}
