// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/promql_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package aggregator

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

type querierMock struct {
	series []*promql.StorageSeries
}

func (m *querierMock) Select(sorted bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return newSeriesIteratorMock(m.series)
}

func (m *querierMock) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m *querierMock) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (m *querierMock) Close() error { return nil }

type seriesIteratorMock struct {
	idx    int
	series []*promql.StorageSeries
}

func newSeriesIteratorMock(series []*promql.StorageSeries) *seriesIteratorMock {
	return &seriesIteratorMock{
		idx:    -1,
		series: series,
	}
}

func (i *seriesIteratorMock) Next() bool {
	i.idx++
	return i.idx < len(i.series)
}

func (i *seriesIteratorMock) At() storage.Series {
	if i.idx >= len(i.series) {
		return nil
	}

	return i.series[i.idx]
}

func (i *seriesIteratorMock) Err() error {
	return nil
}

func (i *seriesIteratorMock) Warnings() storage.Warnings {
	return nil
}
