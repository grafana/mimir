// SPDX-License-Identifier: AGPL-3.0-only

//go:build slicelabels || dedupelabels

package activeseries

import "github.com/prometheus/prometheus/model/labels"

func deletedSeriesLbls(_ string, lbls labels.Labels) labels.Labels {
	return lbls.Copy()
}
