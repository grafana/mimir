// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// TestOperator is an InstantVectorOperator used only in tests.
type TestOperator struct {
	Series                   []labels.Labels
	Data                     []types.InstantVectorSeriesData
	Closed                   bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker
}

var _ types.InstantVectorOperator = &TestOperator{}

func (t *TestOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (t *TestOperator) SeriesMetadata(_ context.Context) ([]types.SeriesMetadata, error) {
	if len(t.Series) == 0 {
		return nil, nil
	}

	seriesPool, err := types.SeriesMetadataSlicePool.Get(len(t.Series), t.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	seriesPool = seriesPool[:len(t.Series)]
	for i, l := range t.Series {
		seriesPool[i].Labels = l
	}
	return seriesPool, nil
}

func (t *TestOperator) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if len(t.Data) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	d := t.Data[0]
	t.Data = t.Data[1:]

	return d, nil
}

func (t *TestOperator) ReleaseUnreadData(memoryConsumptionTracker *limiting.MemoryConsumptionTracker) {
	for _, d := range t.Data {
		types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
	}

	t.Data = nil
}

func (t *TestOperator) Close() {
	// Note that we do not return any unused series data here: it is the responsibility of the test to call ReleaseUnreadData, if needed.
	t.Closed = true
}
