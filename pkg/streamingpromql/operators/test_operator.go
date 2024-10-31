// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// TestOperator is an InstantVectorOperator used only in tests.
type TestOperator struct {
	Series []labels.Labels
	Data   []types.InstantVectorSeriesData
}

var _ types.InstantVectorOperator = &TestOperator{}

func (t *TestOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (t *TestOperator) SeriesMetadata(_ context.Context) ([]types.SeriesMetadata, error) {
	return testutils.LabelsToSeriesMetadata(t.Series), nil
}

func (t *TestOperator) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if len(t.Data) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	d := t.Data[0]
	t.Data = t.Data[1:]

	return d, nil
}

func (t *TestOperator) Close() {
	panic("Close() not supported")
}
