// SPDX-License-Identifier: AGPL-3.0-only

package querier

import "time"

// CardinalityEstimateConfig configures the experimental /api/v1/cardinality/estimate route.
type CardinalityEstimateConfig struct {
	// Timeout is -querier.timeout.
	Timeout time.Duration
}
