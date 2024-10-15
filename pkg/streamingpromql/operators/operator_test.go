// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Operator used only in tests.
type testOperator struct {
	series []labels.Labels
	data   []types.InstantVectorSeriesData
}

var _ types.InstantVectorOperator = &testOperator{}

func (t *testOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (t *testOperator) SeriesMetadata(_ context.Context) ([]types.SeriesMetadata, error) {
	return labelsToSeriesMetadata(t.series), nil
}

func (t *testOperator) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if len(t.data) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	d := t.data[0]
	t.data = t.data[1:]

	return d, nil
}

func (t *testOperator) Close() {
	panic("Close() not supported")
}
