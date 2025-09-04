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
	Finalized                bool
	Closed                   bool
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

var _ types.InstantVectorOperator = &TestOperator{}

func (t *TestOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (t *TestOperator) SeriesMetadata(_ context.Context) ([]types.SeriesMetadata, error) {
	if len(t.Series) == 0 {
		return nil, nil
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
	// Nothing to do.
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
