// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

// The engine's AppenderV2 answers like the TSDB's: zero samples refused out of order, even when they're also too old,
// histograms, staleness markers, and exemplars with partial errors.
func TestEngineAppenderV2MatchesPrometheus(t *testing.T) {
	for _, opts := range []differentialOptions{{maxExemplars: 10}, {oooWindowMs: 30 * 60_000, maxExemplars: 10}} {
		t.Run(fmt.Sprintf("%+v", opts), func(t *testing.T) {
			ctx := context.Background()
			heads := [2]headUnderTest{openPrometheus(t, t.TempDir(), opts, nil), openEngine(t, t.TempDir(), opts, nil)}
			t.Cleanup(func() {
				for _, head := range heads {
					_ = head.Close()
				}
			})
			appenderV2 := func(side int) EngineAppenderV2 {
				if head, ok := heads[side].(prometheusHead); ok {
					return head.db.AppenderV2(ctx).(EngineAppenderV2)
				}
				return heads[side].(*Engine).AppenderV2(ctx)
			}

			floats := promlabels.FromStrings("__name__", "requests_total")
			histograms := promlabels.FromStrings("__name__", "latency")
			zero := storage.AppendV2Options{RejectOutOfOrder: true}
			h := func(count uint64) *histogram.Histogram {
				return &histogram.Histogram{Count: count, Sum: float64(count), Schema: 0, ZeroThreshold: 0.001, ZeroCount: count}
			}
			zeroHistogram := &histogram.Histogram{CounterResetHint: histogram.CounterReset, Schema: 0, ZeroThreshold: 0.001}
			base := int64(10 * chunkRangeMs)

			type call struct {
				name string
				do   func(app EngineAppenderV2) (storage.SeriesRef, error)
			}
			batches := [][]call{
				{
					// The head starts at the first zero sample, as at the TSDB's.
					{"zero on an empty head", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						return app.Append(0, floats, 0, base-30_000, 0, nil, nil, zero)
					}},
					{"float", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						return app.Append(0, floats, 0, base, 1, nil, nil, storage.AppendV2Options{})
					}},
					{"float with exemplars", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						return app.Append(0, floats, 0, base+60_000, 2, nil, nil, storage.AppendV2Options{Exemplars: []exemplar.Exemplar{
							{Labels: promlabels.FromStrings("trace_id", "a"), Value: 2, Ts: base + 60_000, HasTs: true},
						}})
					}},
					{"zero histogram", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						return app.Append(0, histograms, 0, base-30_000, 0, zeroHistogram, nil, zero)
					}},
					{"histogram", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						return app.Append(0, histograms, 0, base, 0, h(1), nil, storage.AppendV2Options{})
					}},
				},
				{
					{"zero out of order", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						return app.Append(0, floats, 0, base+30_000, 0, nil, nil, zero)
					}},
					{"zero too old", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						return app.Append(0, floats, 0, base-2*chunkRangeMs, 0, nil, nil, zero)
					}},
					{"zero histogram out of order", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						return app.Append(0, histograms, 0, base-15_000, 0, zeroHistogram, nil, zero)
					}},
					{"float out of order", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						return app.Append(0, floats, 0, base+30_000, 3, nil, nil, storage.AppendV2Options{})
					}},
					{"histogram staleness marker as a float", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						ref, err := app.Append(0, histograms, 0, base+60_000, 0, h(2), nil, storage.AppendV2Options{})
						if err != nil {
							return ref, err
						}
						return app.Append(ref, histograms, 0, base+120_000, math.Float64frombits(value.StaleNaN), nil, nil, storage.AppendV2Options{})
					}},
					{"exemplars, one out of order", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						ref, _ := app.GetRef(floats, floats.Hash())
						return app.AppendExemplars(ref, floats, []exemplar.Exemplar{
							{Labels: promlabels.FromStrings("trace_id", "b"), Value: 3, Ts: base + 90_000, HasTs: true},
							{Labels: promlabels.FromStrings("trace_id", "c"), Value: 1, Ts: base, HasTs: true},
						})
					}},
					{"exemplars of an unknown series", func(app EngineAppenderV2) (storage.SeriesRef, error) {
						unknown := promlabels.FromStrings("__name__", "unknown")
						return app.AppendExemplars(0, unknown, []exemplar.Exemplar{{Value: 1, Ts: base, HasTs: true}})
					}},
				},
			}
			for _, batch := range batches {
				apps := [2]EngineAppenderV2{appenderV2(0), appenderV2(1)}
				for _, c := range batch {
					var outcomes [2]appendOutcome
					for side, app := range apps {
						outcomes[side] = outcomeOf(c.do(app))
					}
					require.Equal(t, outcomes[0], outcomes[1], c.name)
				}
				for _, app := range apps {
					require.NoError(t, app.Commit())
				}
			}

			all := promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", ".+")
			require.Equal(t, querySamples(t, heads[0], math.MinInt64, math.MaxInt64, nil, all), querySamples(t, heads[1], math.MinInt64, math.MaxInt64, nil, all))
			require.Equal(t, heads[0].MinTime(), heads[1].MinTime())
			require.Equal(t, heads[0].MaxTime(), heads[1].MaxTime())
			var exemplars [2][]exemplar.QueryResult
			for side, head := range heads {
				q, err := head.ExemplarQuerier(ctx)
				require.NoError(t, err)
				exemplars[side], err = q.Select(math.MinInt64, math.MaxInt64, []*promlabels.Matcher{all})
				require.NoError(t, err)
			}
			require.Equal(t, exemplars[0], exemplars[1])
		})
	}
}
