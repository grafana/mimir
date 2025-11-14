// SPDX-License-Identifier: AGPL-3.0-only

package testutil

import "github.com/grafana/mimir/pkg/mimirpb"

// RemoveEmptyObjectFromSeries is a test utility to replace some empty fields
// inside the given series with `nil` for easy comparison.
func RemoveEmptyObjectFromSeries(series []mimirpb.PreallocTimeseries) []mimirpb.PreallocTimeseries {
	for i := range series {
		s := series[i].TimeSeries
		// Clean extra empty objects due to pooling.
		if len(s.Samples) == 0 {
			s.Samples = nil
		}
		if len(s.Histograms) == 0 {
			s.Histograms = nil
		}
		if len(s.Exemplars) == 0 {
			s.Exemplars = nil
		}
	}
	return series
}
