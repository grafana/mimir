// SPDX-License-Identifier: AGPL-3.0-only

package queryrange

import (
	"time"
)

type Limits interface {
	// MaxQueryLookback returns the max lookback period of queries.
	MaxQueryLookback(userID string) time.Duration

	// MaxQueryLength returns the limit of the length (in time) of a query.
	MaxQueryLength(string) time.Duration

	// MaxQueryParallelism returns the limit to the number of split queries the
	// frontend will process in parallel.
	MaxQueryParallelism(string) int

	// MaxCacheFreshness returns the period after which results are cacheable,
	// to prevent caching of very recent results.
	MaxCacheFreshness(string) time.Duration
}
