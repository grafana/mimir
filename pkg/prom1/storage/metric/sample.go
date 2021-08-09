// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package metric

import "github.com/prometheus/common/model"

// Interval describes the inclusive interval between two Timestamps.
type Interval struct {
	OldestInclusive model.Time
	NewestInclusive model.Time
}
