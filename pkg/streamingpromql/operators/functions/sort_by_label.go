// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"sort"

	"github.com/facette/natsort"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type SortByLabel struct {
	sortBase

	labels []string
}

var _ types.InstantVectorOperator = &SortByLabel{}

func NewSortByLabel(
	inner types.InstantVectorOperator,
	labels []string,
	descending bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,

) *SortByLabel {
	return &SortByLabel{
		sortBase: newSortBase(inner, descending, memoryConsumptionTracker, expressionPosition),
		labels:   labels,
	}
}

func (s *SortByLabel) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	allSeries, err := s.inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	s.allData = make([]types.InstantVectorSeriesData, len(allSeries))
	for idx := range allSeries {
		d, err := s.inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		s.allData[idx] = d
	}

	sort.Sort(&sortSeriesAndData{
		series: allSeries,
		data:   s.allData,
		less: func(series []types.SeriesMetadata, data []types.InstantVectorSeriesData, i, j int) bool {
			labels1 := series[i].Labels
			labels2 := series[j].Labels

			for _, lbl := range s.labels {
				val1 := labels1.Get(lbl)
				val2 := labels2.Get(lbl)
				if val1 == val2 {
					continue
				}

				if s.descending {
					return natsort.Compare(val2, val1)
				}
				return natsort.Compare(val1, val2)
			}

			// If all labels provided as arguments were equal, sort by the full label set. This ensures a
			// consistent ordering and matches the behavior of the Prometheus engine.
			if s.descending {
				return labels.Compare(labels2, labels1) < 0
			}
			return labels.Compare(labels1, labels2) < 0
		},
	})

	return allSeries, nil
}
