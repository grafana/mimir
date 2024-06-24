// SPDX-License-Identifier: AGPL-3.0-only

//go:generate ./generate.sh

package otlp

import (
	// Ensure that prometheusremotewrite sources are vendored for generator script.
	_ "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// TimeSeries returns a slice of the mimirpb.TimeSeries that were converted from OTel format.
func (c *MimirConverter) TimeSeries() []mimirpb.PreallocTimeseries {
	totalCount := len(c.unique)
	for _, ts := range c.conflicts {
		totalCount += len(ts)
	}

	allTS := mimirpb.PreallocTimeseriesSliceFromPool()[:0]
	if cap(allTS) < totalCount {
		allTS = make([]mimirpb.PreallocTimeseries, 0, totalCount)
	}
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
