// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
)

// Operator used only in tests.
type testOperator struct {
	series []labels.Labels
	data   []InstantVectorSeriesData
}

func (t *testOperator) SeriesMetadata(_ context.Context) ([]SeriesMetadata, error) {
	return labelsToSeriesMetadata(t.series), nil
}

func (t *testOperator) NextSeries(_ context.Context) (InstantVectorSeriesData, error) {
	if len(t.data) == 0 {
		return InstantVectorSeriesData{}, EOS
	}

	d := t.data[0]
	t.data = t.data[1:]

	return d, nil
}

func (t *testOperator) Close() {
	panic("Close() not supported")
}
