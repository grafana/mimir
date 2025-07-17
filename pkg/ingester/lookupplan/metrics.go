// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	planDuration *prometheus.HistogramVec
	// TODO dimitarvdimitrov measure q-error
}

// TODO dimitarvdimitrov add constructor
