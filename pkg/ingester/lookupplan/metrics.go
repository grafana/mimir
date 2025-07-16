// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	planningDuration *prometheus.HistogramVec
}

// TODO dimitarvdimitrov add constructor
