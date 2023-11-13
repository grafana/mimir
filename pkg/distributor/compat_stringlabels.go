// SPDX-License-Identifier: AGPL-3.0-only

//go:build stringlabels

package distributor

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// activeSeriesResponse is a helper to merge/deduplicate ActiveSeries responses from ingesters.
type activeSeriesResponse struct {
	m      sync.Mutex
	series map[labels.Labels]struct{}
}

func newActiveSeriesResponse() *activeSeriesResponse {
	return &activeSeriesResponse{
		series: make(map[labels.Labels]struct{}),
	}
}

func (r *activeSeriesResponse) add(series []*mimirpb.Metric) {
	r.m.Lock()
	defer r.m.Unlock()

	for _, metric := range series {
		r.series[mimirpb.FromLabelAdaptersToLabels(metric.Labels)] = struct{}{}
	}
}

func (r *activeSeriesResponse) result() []labels.Labels {
	r.m.Lock()
	defer r.m.Unlock()

	result := make([]labels.Labels, 0, len(r.series))
	for series := range r.series {
		result = append(result, series)
	}
	return result
}
