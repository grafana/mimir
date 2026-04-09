// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"time"

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

func (t *TestOperator) Stats(_ context.Context) (*types.OperatorEvaluationStats, error) {
	panic("not implemented")
}

func (t *TestOperator) Close() {
	// Note that we do not return any unused series data here: it is the responsibility of the test to call ReleaseUnreadData, if needed.
	t.Closed = true
}

type TestRangeOperator struct {
	Series                     []labels.Labels
	DropName                   []bool
	CurrentSeriesIndex         int
	Data                       []types.InstantVectorSeriesData
	StepRange                  time.Duration
	HaveReadCurrentStepSamples bool
	Floats                     *types.FPointRingBuffer
	FloatsView                 *types.FPointRingBufferView
	Histograms                 *types.HPointRingBuffer
	HistogramsView             *types.HPointRingBufferView

	Finalized        bool
	Closed           bool
	MatchersProvided types.Matchers

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

var _ types.RangeVectorOperator = &TestRangeOperator{}

func NewTestRangeOperator(series []labels.Labels, dropName []bool, data []types.InstantVectorSeriesData, stepRange time.Duration, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *TestRangeOperator {
	return &TestRangeOperator{
		Series:                   series,
		DropName:                 dropName,
		CurrentSeriesIndex:       -1,
		Data:                     data,
		StepRange:                stepRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (t *TestRangeOperator) SeriesMetadata(_ context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	t.MatchersProvided = matchers

	if len(t.Series) == 0 {
		return nil, nil
	}

	metadata, err := types.SeriesMetadataSlicePool.Get(len(t.Series), t.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	metadata = metadata[:len(t.Series)]

	for i, l := range t.Series {
		metadata[i].Labels = l
		err := t.memoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(l)
		if err != nil {
			return nil, err
		}

		if t.DropName != nil && t.DropName[i] {
			metadata[i].DropName = true
		}
	}

	return metadata, nil
}

func (t *TestRangeOperator) NextSeries(_ context.Context) error {
	if t.CurrentSeriesIndex+1 >= len(t.Series) {
		return types.EOS
	}

	t.HaveReadCurrentStepSamples = false
	t.CurrentSeriesIndex++

	return nil
}

func (t *TestRangeOperator) NextStepSamples(_ context.Context) (*types.RangeVectorStepData, error) {
	if t.HaveReadCurrentStepSamples {
		return nil, types.EOS
	}

	t.HaveReadCurrentStepSamples = true

	if t.Floats == nil {
		t.Floats = types.NewFPointRingBuffer(t.memoryConsumptionTracker)
	}

	if t.Histograms == nil {
		t.Histograms = types.NewHPointRingBuffer(t.memoryConsumptionTracker)
	}

	d := t.Data[t.CurrentSeriesIndex]
	endT := t.StepRange.Milliseconds()

	t.Floats.Reset()
	for _, p := range d.Floats {
		if err := t.Floats.Append(p); err != nil {
			return nil, err
		}
	}
	t.FloatsView = t.Floats.ViewUntilSearchingBackwards(endT, t.FloatsView)

	t.Histograms.Reset()
	for _, p := range d.Histograms {
		if err := t.Histograms.Append(p); err != nil {
			return nil, err
		}
	}
	t.HistogramsView = t.Histograms.ViewUntilSearchingBackwards(endT, t.HistogramsView)

	return &types.RangeVectorStepData{
		Floats:     t.FloatsView,
		Histograms: t.HistogramsView,
		StepT:      endT,
		RangeStart: 0,
		RangeEnd:   endT,
	}, nil
}

func (t *TestRangeOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (t *TestRangeOperator) Prepare(_ context.Context, _ *types.PrepareParams) error {
	// Nothing to do.
	return nil
}

func (t *TestRangeOperator) AfterPrepare(_ context.Context) error {
	return nil
}

func (t *TestRangeOperator) Finalize(_ context.Context) error {
	t.Finalized = true

	if t.Floats != nil {
		t.Floats.Close()
	}

	if t.Histograms != nil {
		t.Histograms.Close()
	}

	t.Floats = nil
	t.FloatsView = nil
	t.Histograms = nil
	t.HistogramsView = nil

	return nil
}

func (t *TestRangeOperator) Stats(_ context.Context) (*types.OperatorEvaluationStats, error) {
	panic("not implemented")
}

func (t *TestRangeOperator) Close() {
	t.Closed = true
}
