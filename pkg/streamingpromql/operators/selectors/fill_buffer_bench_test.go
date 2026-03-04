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
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// buildLoadLine creates a promqltest load string with numSamples samples at interval scrapeInterval.
func buildLoadLine(metricName string, scrapeInterval time.Duration, numSamples int) string {
	s := "load " + scrapeInterval.String() + "\n  " + metricName
	for i := 0; i < numSamples; i++ {
		s += " " + string(rune('1'+i%9))
	}
	return s
}

// BenchmarkFillBuffer_SteadyState measures the common steady-state case: a sliding range
// query over a moderately dense series where the ring buffer already holds most samples
// and fillBuffer only needs to fetch the next one or two per step.
func BenchmarkFillBuffer_SteadyState(b *testing.B) {
	const (
		scrapeInterval = 15 * time.Second
		rangeDuration  = 5 * time.Minute
		step           = time.Minute
		totalDuration  = 30 * time.Minute
	)
	numSamples := int(totalDuration / scrapeInterval) // 120

	storage := promqltest.LoadedStorage(b, buildLoadLine("bench_metric", scrapeInterval, numSamples))
	ctx := context.Background()
	start := time.Unix(0, 0).Add(rangeDuration)
	end := time.Unix(0, 0).Add(totalDuration)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mc := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
		tr := types.NewRangeQueryTimeRange(start, end, step)
		sel := &Selector{
			Queryable:                storage,
			TimeRange:                tr,
			Matchers:                 []types.Matcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "bench_metric"}},
			LookbackDelta:            5 * time.Minute,
			Range:                    rangeDuration,
			MemoryConsumptionTracker: mc,
		}
		rv := NewRangeVectorSelector(sel, mc, types.NewQueryStats())
		_, err := rv.SeriesMetadata(ctx, nil)
		require.NoError(b, err)
		require.NoError(b, rv.NextSeries(ctx))

		for {
			_, err := rv.NextStepSamples(ctx)
			if errors.Is(err, types.EOS) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkFillBuffer_InitialSeek measures the case where the first step of a range query
// must skip a large number of samples before the range window.
func BenchmarkFillBuffer_InitialSeek(b *testing.B) {
	const (
		scrapeInterval = 15 * time.Second
		rangeDuration  = 5 * time.Minute
		step           = time.Minute
		totalDuration  = 60 * time.Minute
	)
	numSamples := int(totalDuration / scrapeInterval) // 240 samples

	storage := promqltest.LoadedStorage(b, buildLoadLine("skip_metric", scrapeInterval, numSamples))
	ctx := context.Background()

	queryStart := time.Unix(0, 0).Add(totalDuration - rangeDuration)
	queryEnd := time.Unix(0, 0).Add(totalDuration)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mc := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
		tr := types.NewRangeQueryTimeRange(queryStart, queryEnd, step)
		sel := &Selector{
			Queryable:                storage,
			TimeRange:                tr,
			Matchers:                 []types.Matcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "skip_metric"}},
			LookbackDelta:            5 * time.Minute,
			Range:                    rangeDuration,
			MemoryConsumptionTracker: mc,
		}
		rv := NewRangeVectorSelector(sel, mc, types.NewQueryStats())
		_, err := rv.SeriesMetadata(ctx, nil)
		require.NoError(b, err)
		require.NoError(b, rv.NextSeries(ctx))

		for {
			_, err := rv.NextStepSamples(ctx)
			if errors.Is(err, types.EOS) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkFillBuffer_ManySeriesInitialSeek benchmarks many series each requiring
// an initial seek past pre-window samples.
func BenchmarkFillBuffer_ManySeriesInitialSeek(b *testing.B) {
	const (
		scrapeInterval = 15 * time.Second
		rangeDuration  = 5 * time.Minute
		step           = time.Minute
		totalDuration  = 60 * time.Minute
		numSeries      = 20
	)
	numSamples := int(totalDuration / scrapeInterval) // 240 per series

	loadData := "load " + scrapeInterval.String() + "\n"
	for i := 0; i < numSeries; i++ {
		line := fmt.Sprintf("  mseries_metric{instance=\"%d\"}", i)
		for j := 0; j < numSamples; j++ {
			line += " " + string(rune('1'+j%9))
		}
		loadData += line + "\n"
	}

	storage := promqltest.LoadedStorage(b, loadData)
	ctx := context.Background()

	queryStart := time.Unix(0, 0).Add(totalDuration - rangeDuration)
	queryEnd := time.Unix(0, 0).Add(totalDuration)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mc := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
		tr := types.NewRangeQueryTimeRange(queryStart, queryEnd, step)
		sel := &Selector{
			Queryable:                storage,
			TimeRange:                tr,
			Matchers:                 []types.Matcher{{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "mseries_metric"}},
			LookbackDelta:            5 * time.Minute,
			Range:                    rangeDuration,
			MemoryConsumptionTracker: mc,
		}
		rv := NewRangeVectorSelector(sel, mc, types.NewQueryStats())
		metadata, err := rv.SeriesMetadata(ctx, nil)
		require.NoError(b, err)

		for range metadata {
			require.NoError(b, rv.NextSeries(ctx))
			for {
				_, err := rv.NextStepSamples(ctx)
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
