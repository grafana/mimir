// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// TestOperator is an InstantVectorOperator used only in tests.
type TestOperator struct {
	Series                   []labels.Labels
	DropName                 []bool
	Data                     []types.InstantVectorSeriesData
	Prepared                 bool
	Finalized                bool
	Closed                   bool
	Position                 posrange.PositionRange
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	MatchersProvided types.Matchers
}

var _ types.InstantVectorOperator = &TestOperator{}

func (t *TestOperator) ExpressionPosition() posrange.PositionRange {
	return t.Position
}

func (t *TestOperator) SeriesMetadata(_ context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	t.MatchersProvided = matchers

	if len(t.Series) == 0 {
		return nil, nil
	}

	if len(matchers) != 0 {
		promMatchers, err := matchers.ToPrometheusType()
		if err != nil {
			return nil, err
		}

		// If we've been passed extra matchers to apply at runtime, adjust the series metadata
		// and data to remove anything that doesn't match. This simulates how the matchers would
		// be applied in a real operator.
		for i := 0; i < len(t.Series); {
			if !t.matches(t.Series[i], promMatchers) {
				t.Series = append(t.Series[:i], t.Series[i+1:]...)
				t.Data = append(t.Data[:i], t.Data[i+1:]...)
			} else {
				i++
			}
		}
	}

	metadata, err := types.SeriesMetadataSlicePool.Get(len(t.Series), t.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	metadata = metadata[:len(t.Series)]
	for i, l := range t.Series {
		metadata[i].Labels = l
		err := t.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(l)
		if err != nil {
			return nil, err
		}
		if t.DropName != nil && t.DropName[i] {
			metadata[i].DropName = true
		}
	}
	return metadata, nil
}

func (t *TestOperator) matches(series labels.Labels, matchers []*labels.Matcher) bool {
	matches := true
	for _, m := range matchers {
		series.Range(func(l labels.Label) {
			if l.Name == m.Name && !m.Matches(l.Value) {
				matches = false
			}
		})
	}
	return matches
}

func (t *TestOperator) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if len(t.Data) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	d := t.Data[0]
	t.Data = t.Data[1:]

	return d, nil
}

func (t *TestOperator) ReleaseUnreadData(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	for _, d := range t.Data {
		types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
	}

	t.Data = nil
}

func (t *TestOperator) Prepare(_ context.Context, _ *types.PrepareParams) error {
	t.Prepared = true
	return nil
}

func (t *TestOperator) AfterPrepare(_ context.Context) error {
	return nil
}

func (t *TestOperator) Finalize(_ context.Context) error {
	t.Finalized = true
	return nil
}

func (t *TestOperator) Close() {
	// Note that we do not return any unused series data here: it is the responsibility of the test to call ReleaseUnreadData, if needed.
	t.Closed = true
}
