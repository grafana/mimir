// SPDX-License-Identifier: AGPL-3.0-only

package selectors

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// BenchmarkNextStepSamples measures the per-step cost of NextStepSamples in the
// common (non-smoothed, non-anchored) range-vector case.
func BenchmarkNextStepSamples(b *testing.B) {
	const numSeries = 20
	const scrapeIntervalSecs = 15
	const durationMinutes = 10
	const samplesPerSeries = (durationMinutes * 60) / scrapeIntervalSecs // 40

	loadStmt := fmt.Sprintf("load %ds\n", scrapeIntervalSecs)
	for i := 0; i < numSeries; i++ {
		loadStmt += fmt.Sprintf("  metric{i=\"%d\"} 0+1x%d\n", i, samplesPerSeries)
	}

	storage := promqltest.LoadedStorage(b, loadStmt)
	b.Cleanup(func() { _ = storage.Close() })

	ctx := context.Background()
	queryStart := time.Unix(0, 0)
	queryEnd := queryStart.Add(time.Duration(durationMinutes) * time.Minute)
	step := 30 * time.Second
	rangeDuration := 5 * time.Minute

	// Determine series count once.
	mc0 := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	tr0 := types.NewRangeQueryTimeRange(queryStart, queryEnd, step)
	sel0 := &Selector{
		Queryable:                storage,
		TimeRange:                tr0,
		Matchers:                 []types.Matcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "metric"}},
		LookbackDelta:            5 * time.Minute,
		Range:                    rangeDuration,
		MemoryConsumptionTracker: mc0,
	}
	rv0 := NewRangeVectorSelector(sel0, mc0, types.NewQueryStats())
	meta0, err := rv0.SeriesMetadata(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	numFound := len(meta0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mc2 := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
		tr2 := types.NewRangeQueryTimeRange(queryStart, queryEnd, step)
		sel2 := &Selector{
			Queryable:                storage,
			TimeRange:                tr2,
			Matchers:                 []types.Matcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "metric"}},
			LookbackDelta:            5 * time.Minute,
			Range:                    rangeDuration,
			MemoryConsumptionTracker: mc2,
		}
		rv2 := NewRangeVectorSelector(sel2, mc2, types.NewQueryStats())
		if _, err2 := rv2.SeriesMetadata(ctx, nil); err2 != nil {
			b.Fatal(err2)
		}
		b.StartTimer()

		for range numFound {
			if err := rv2.NextSeries(ctx); err != nil {
				b.Fatal(err)
			}
			for {
				_, err := rv2.NextStepSamples(ctx)
				if errors.Is(err, types.EOS) {
					break
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	}
}

// BenchmarkViewUntilSearchingBackwards benchmarks the old O(k) backward-scan
// view method. Pair with BenchmarkViewUntilWithSinglePointOvershoot to see the
// isolated gain from the optimisation.
func BenchmarkViewUntilSearchingBackwards(b *testing.B) {
	for _, n := range []int{8, 32, 128} {
		n := n
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			mc := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			buf := types.NewFPointRingBuffer(mc)
			for i := 0; i < n; i++ {
				if err := buf.Append(promql.FPoint{T: int64(i * 1000), F: float64(i)}); err != nil {
					b.Fatal(err)
				}
			}
			maxT := int64((n - 2) * 1000) // last point is past maxT
			var view *types.FPointRingBufferView
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				view = buf.ViewUntilSearchingBackwards(maxT, view)
			}
			_ = view
		})
	}
}

// BenchmarkViewUntilWithSinglePointOvershoot benchmarks the new O(1) view method
// that exploits the guarantee that fillBuffer appends at most one point past maxT.
func BenchmarkViewUntilWithSinglePointOvershoot(b *testing.B) {
	for _, n := range []int{8, 32, 128} {
		n := n
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			mc := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			buf := types.NewFPointRingBuffer(mc)
			for i := 0; i < n; i++ {
				if err := buf.Append(promql.FPoint{T: int64(i * 1000), F: float64(i)}); err != nil {
					b.Fatal(err)
				}
			}
			maxT := int64((n - 2) * 1000) // last point is past maxT
			var view *types.FPointRingBufferView
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				view = buf.ViewUntilWithSinglePointOvershoot(maxT, view)
			}
			_ = view
		})
	}
}
