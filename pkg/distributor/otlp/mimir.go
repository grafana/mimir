// SPDX-License-Identifier: AGPL-3.0-only

//go:generate ./cmd/generate/generate

package otlp

import (
	"strconv"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// TimeSeries returns a slice of the mimirpb.TimeSeries that were converted from OTel format.
func (c *MimirConverter) TimeSeries() []mimirpb.PreallocTimeseries {
	conflicts := 0
	for _, ts := range c.conflicts {
		conflicts += len(ts)
	}
	allTS := mimirpb.PreallocTimeseriesSliceFromPool()
	for _, ts := range c.unique {
		allTS = append(allTS, mimirpb.PreallocTimeseries{TimeSeries: ts})
	}
	for _, cTS := range c.conflicts {
		for _, ts := range cTS {
			allTS = append(allTS, mimirpb.PreallocTimeseries{TimeSeries: ts})
		}
	}

	return allTS
}

// FromMetrics converts pmetric.Metrics to Prometheus remote write format.
//
// Deprecated: FromMetrics exists for historical compatibility and should not be used.
// Use instead Mimir.FromMetrics() and then MimirConverter.TimeSeries()
// to obtain the Prometheus remote write format metrics.
func FromMetrics(md pmetric.Metrics, settings Settings) (map[string]*mimirpb.TimeSeries, error) {
	c := NewMimirConverter()
	errs := c.FromMetrics(md, settings)
	tss := c.TimeSeries()
	out := make(map[string]*mimirpb.TimeSeries, len(tss))
	for i := range tss {
		out[strconv.Itoa(i)] = tss[i].TimeSeries
	}

	return out, errs
}
