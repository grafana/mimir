// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/test_utils.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/test_utils_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/util/test"
)

// genLabels will create a slice of labels where each label has an equal chance to occupy a value from [0,labelBuckets]. It returns a slice of length labelBuckets^len(labelSet)
func genLabels(
	labelSet []string,
	labelBuckets int,
) (result [][]labels.Label) {
	if len(labelSet) == 0 {
		return result
	}

	l := labelSet[0]
	rest := genLabels(labelSet[1:], labelBuckets)

	for i := 0; i < labelBuckets; i++ {
		x := labels.Label{
			Name:  l,
			Value: fmt.Sprintf("%d", i),
		}
		if len(rest) == 0 {
			set := []labels.Label{x}
			result = append(result, set)
			continue
		}
		for _, others := range rest {
			set := append(others, x)
			result = append(result, set)
		}
	}
	return result

}

// newMockShardedQueryable creates a shard-aware in memory queryable.
func newMockShardedQueryable(
	nSamples int,
	nHistograms int,
	labelSet []string,
	labelBuckets int,
	delayPerSeries time.Duration,
) *mockShardedQueryable {
	samples := make([]model.SamplePair, 0, nSamples)
	for i := 0; i < nSamples; i++ {
		samples = append(samples, model.SamplePair{
			Timestamp: model.Time(i * 1000),
			Value:     model.SampleValue(i),
		})
	}
	histograms := make([]mimirpb.Histogram, 0, nHistograms)
	for i := 0; i < nHistograms; i++ {
		histograms = append(histograms, mimirpb.FromHistogramToHistogramProto(int64(i*1000), test.GenerateTestHistogram(i)))
	}
	sets := genLabels(labelSet, labelBuckets)
	xs := make([]storage.Series, 0, len(sets))
	for _, ls := range sets {
		xs = append(xs, series.NewConcreteSeries(labels.New(ls...), samples, histograms))
	}

	return &mockShardedQueryable{
		series:         xs,
		delayPerSeries: delayPerSeries,
	}
}

// mockShardedQueryable is exported to be reused in the querysharding benchmarking
type mockShardedQueryable struct {
	series         []storage.Series
	delayPerSeries time.Duration
}

// Querier impls storage.Queryable
func (q *mockShardedQueryable) Querier(_, _ int64) (storage.Querier, error) {
	return q, nil
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q *mockShardedQueryable) Select(_ context.Context, _ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	tStart := time.Now()

	shard, _, err := sharding.ShardFromMatchers(matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	var (
		start int
		end   int
	)

	if shard == nil {
		start = 0
		end = len(q.series)
	} else {
		// return the series range associated with this shard
		seriesPerShard := len(q.series) / int(shard.ShardCount)
		start = int(shard.ShardIndex) * seriesPerShard
		end = start + seriesPerShard

		// if we're clipping an odd # of series, add the final series to the last shard
		if end == len(q.series)-1 && len(q.series)%2 == 1 {
			end = len(q.series)
		}
	}

	var name string
	for _, m := range matchers {
		if m.Type == labels.MatchEqual && m.Name == "__name__" {
			name = m.Value
		}
	}

	results := make([]storage.Series, 0, end-start)
	for i := start; i < end; i++ {
		results = append(results, &shardLabelSeries{
			shard:  shard,
			name:   name,
			Series: q.series[i],
		})
	}

	// loosely enforce the assumption that an operation on 1/nth of the data
	// takes 1/nth of the time.
	duration := q.delayPerSeries * time.Duration(len(q.series))
	if shard != nil {
		duration = duration / time.Duration(shard.ShardCount)
	}

	remaining := time.Until(tStart.Add(duration))
	if remaining > 0 {
		time.Sleep(remaining)
	}

	// sorted
	return series.NewConcreteSeriesSetFromUnsortedSeries(results)
}

// shardLabelSeries allows extending a Series with new labels. This is helpful for adding cortex shard labels
type shardLabelSeries struct {
	shard *sharding.ShardSelector
	name  string
	storage.Series
}

// Labels impls storage.Series
func (s *shardLabelSeries) Labels() labels.Labels {
	ls := s.Series.Labels()
	b := labels.NewBuilder(ls)

	if s.name != "" {
		b.Set("__name__", s.name)
	}

	if s.shard != nil {
		l := s.shard.Label()
		b.Set(l.Name, l.Value)
	}

	return b.Labels()
}

// LabelValues impls storage.Querier
func (q *mockShardedQueryable) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, errors.Errorf("unimplemented")
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (q *mockShardedQueryable) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, errors.Errorf("unimplemented")
}

// Close releases the resources of the Querier.
func (q *mockShardedQueryable) Close() error {
	return nil
}

func TestGenLabelsCorrectness(t *testing.T) {
	ls := genLabels([]string{"a", "b"}, 2)
	expected := []labels.Labels{
		labels.FromStrings("a", "0", "b", "0"),
		labels.FromStrings("a", "0", "b", "1"),
		labels.FromStrings("a", "1", "b", "0"),
		labels.FromStrings("a", "1", "b", "1"),
	}
	for i, x := range expected {
		got := labels.New(ls[i]...)
		require.Equal(t, x, got)
	}
}

func TestGenLabelsSize(t *testing.T) {
	for _, tc := range []struct {
		set     []string
		buckets int
	}{
		{
			set:     []string{"a", "b"},
			buckets: 5,
		},
		{
			set:     []string{"a", "b", "c"},
			buckets: 10,
		},
	} {
		sets := genLabels(tc.set, tc.buckets)
		require.Equal(
			t,
			math.Pow(float64(tc.buckets), float64(len(tc.set))),
			float64(len(sets)),
		)
	}
}

func TestNewMockShardedQueryable(t *testing.T) {
	for _, tc := range []struct {
		shards                              uint64
		nSamples, nHistograms, labelBuckets int
		labelSet                            []string
	}{
		{
			nSamples:     100,
			nHistograms:  30,
			shards:       1,
			labelBuckets: 3,
			labelSet:     []string{"a", "b", "c"},
		},
		{
			nSamples:     0,
			nHistograms:  0,
			shards:       2,
			labelBuckets: 3,
			labelSet:     []string{"a", "b", "c"},
		},
	} {
		q := newMockShardedQueryable(tc.nSamples, tc.nHistograms, tc.labelSet, tc.labelBuckets, 0)
		expectedSeries := int(math.Pow(float64(tc.labelBuckets), float64(len(tc.labelSet))))

		ctx := context.Background()
		seriesCt := 0
		for i := uint64(0); i < tc.shards; i++ {

			set := q.Select(ctx, false, nil, &labels.Matcher{
				Type: labels.MatchEqual,
				Name: sharding.ShardLabel,
				Value: sharding.ShardSelector{
					ShardIndex: i,
					ShardCount: tc.shards,
				}.LabelValue(),
			})

			require.NoError(t, set.Err())

			var iter chunkenc.Iterator
			for set.Next() {
				seriesCt++
				iter = set.At().Iterator(iter)
				samples := 0
				histograms := 0
				for valType := iter.Next(); valType != chunkenc.ValNone; valType = iter.Next() {
					switch valType {
					case chunkenc.ValFloat:
						samples++
					case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
						histograms++
					}
				}
				require.Equal(t, tc.nSamples, samples)
				require.Equal(t, tc.nHistograms, histograms)
			}

		}
		require.Equal(t, expectedSeries, seriesCt)
	}
}

type engineOpt func(o *streamingpromql.EngineOpts)

func withTimeout(timeout time.Duration) engineOpt {
	return func(o *streamingpromql.EngineOpts) {
		o.CommonOpts.Timeout = timeout
	}
}

func withMaxSamples(samples int) engineOpt {
	return func(o *streamingpromql.EngineOpts) {
		o.CommonOpts.MaxSamples = samples
	}
}

func newEngineForTesting(t *testing.T, engine string, opts ...engineOpt) (promql.EngineOpts, promql.QueryEngine) {
	t.Helper()

	mqeOpts := streamingpromql.NewTestEngineOpts()
	for _, o := range opts {
		o(&mqeOpts)
	}

	promOpts := mqeOpts.CommonOpts

	switch engine {
	case querier.PrometheusEngine:
		return promOpts, promql.NewEngine(promOpts)
	case querier.MimirEngine:
		metrics := stats.NewQueryMetrics(promOpts.Reg)
		planner, err := streamingpromql.NewQueryPlanner(mqeOpts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)
		eng, err := streamingpromql.NewEngine(mqeOpts, metrics, planner)
		if err != nil {
			t.Fatalf("error creating MQE engine for testing: %s", err)
		}

		return promOpts, eng
	default:
		t.Fatalf("invalid promql engine: %v", engine)
	}

	panic("unreachable")
}

// runForEngines runs the provided test closure with the Prometheus Engine and Mimir Query Engine.
func runForEngines(t *testing.T, run func(t *testing.T, opts promql.EngineOpts, eng promql.QueryEngine)) {
	t.Helper()

	promOpts, promEngine := newEngineForTesting(t, querier.PrometheusEngine)
	mqeOpts, mqeEngine := newEngineForTesting(t, querier.MimirEngine)

	engines := map[string]struct {
		engine  promql.QueryEngine
		options promql.EngineOpts
	}{
		querier.PrometheusEngine: {
			engine:  promEngine,
			options: promOpts,
		},
		querier.MimirEngine: {
			engine:  mqeEngine,
			options: mqeOpts,
		},
	}

	for name, tc := range engines {
		t.Run(fmt.Sprintf("engine=%s", name), func(t *testing.T) {
			run(t, tc.options, tc.engine)
		})
	}
}
