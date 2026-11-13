// SPDX-License-Identifier: AGPL-3.0-only

package cost

import "context"

// Sampler provides bounded sample-density and point-size measurements.
// Implementations enforce sampling budgets before fetching or decoding data.
type Sampler interface {
	SampleQueryCost(context.Context, *SamplingRequest) (*SamplingResponse, error)
}
