// SPDX-License-Identifier: AGPL-3.0-only

//go:generate go run ./cmd/generate

package otlp

import (
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
