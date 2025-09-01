// SPDX-License-Identifier: AGPL-3.0-only

package topkbottomk

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestAggregations_ReturnIncompleteGroupsOnEarlyClose(t *testing.T) {
	inputSeries := []labels.Labels{
		labels.FromStrings("idx", "1"),
		labels.FromStrings("idx", "2"),
		labels.FromStrings("idx", "3"),
		labels.FromStrings("idx", "4"),
		labels.FromStrings("idx", "5"),
		labels.FromStrings("idx", "6"),
	}

	expectedOutputSeriesForInstantQuery := []labels.Labels{
		labels.FromStrings("idx", "1"),
		// idx="2" contains histograms and so will be ignored.
		labels.FromStrings("idx", "3"),
		labels.FromStrings("idx", "4"),
		// idx="5" contains histograms and so will be ignored.
		labels.FromStrings("idx", "6"),
	}

	rangeQueryTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(0).Add(5*time.Minute), time.Minute)
	instantQueryTimeRange := types.NewInstantQueryTimeRange(timestamp.Time(0))

	for name, isTopK := range map[string]bool{"topk": true, "bottomk": false} {
		t.Run(name, func(t *testing.T) {
			for name, timeRange := range map[string]types.QueryTimeRange{"range query": rangeQueryTimeRange, "instant query": instantQueryTimeRange} {
				t.Run(name, func(t *testing.T) {
					for name, readSeries := range map[string]bool{"read one series": true, "read no series": false} {
						t.Run(name, func(t *testing.T) {
							ctx := context.Background()
							memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

							inner := &operators.TestOperator{
								Series: inputSeries,
								Data: []types.InstantVectorSeriesData{
									createDummyData(t, false, timeRange, memoryConsumptionTracker),
									createDummyData(t, true, timeRange, memoryConsumptionTracker),
									createDummyData(t, false, timeRange, memoryConsumptionTracker),
									createDummyData(t, false, timeRange, memoryConsumptionTracker),
									createDummyData(t, true, timeRange, memoryConsumptionTracker),
									createDummyData(t, false, timeRange, memoryConsumptionTracker),
								},
								MemoryConsumptionTracker: memoryConsumptionTracker,
							}

							param := scalars.NewScalarConstant(6, timeRange, memoryConsumptionTracker, posrange.PositionRange{})
							o := New(inner, param, timeRange, nil, false, isTopK, memoryConsumptionTracker, annotations.New(), posrange.PositionRange{})

							series, err := o.SeriesMetadata(ctx, nil)
							require.NoError(t, err)

							if timeRange.IsInstant {
								// Instant queries will not return series with only histograms.
								require.ElementsMatch(t, testutils.LabelsToSeriesMetadata(expectedOutputSeriesForInstantQuery), series)
							} else {
								// Range queries will return all input series, but those with histograms will return no data from NextSeries() below.
								require.ElementsMatch(t, testutils.LabelsToSeriesMetadata(inputSeries), series)
							}
							types.SeriesMetadataSlicePool.Put(&series, memoryConsumptionTracker)

							if readSeries {
								seriesData, err := o.NextSeries(ctx)
								require.NoError(t, err)
								types.PutInstantVectorSeriesData(seriesData, memoryConsumptionTracker)
							}

							// TestOperator does not release any unread data on Close(), so do that now.
							for _, d := range inner.Data {
								types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
							}

							// Close the operator and confirm all memory has been released.
							o.Close()
							require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
						})
					}
				})
			}
		})
	}
}

func createDummyData(t *testing.T, histograms bool, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) types.InstantVectorSeriesData {
	d := types.InstantVectorSeriesData{}

	if histograms {
		var err error
		d.Histograms, err = types.HPointSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		require.NoError(t, err)

		for i := range timeRange.StepCount {
			d.Histograms = append(d.Histograms, promql.HPoint{T: timeRange.IndexTime(int64(i)), H: &histogram.FloatHistogram{Count: float64(i)}})
		}

	} else {
		var err error
		d.Floats, err = types.FPointSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		require.NoError(t, err)

		for i := range timeRange.StepCount {
			d.Floats = append(d.Floats, promql.FPoint{T: timeRange.IndexTime(int64(i)), F: float64(i)})
		}
	}

	return d
}
