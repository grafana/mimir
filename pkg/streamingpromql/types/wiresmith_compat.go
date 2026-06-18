// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"fmt"
)

// GoString preserves the gogoslick-generated API: still-gogo-compiled importers
// of types.proto (pkg/querier/querierpb, pkg/streamingpromql/optimize/plan/
// rangevectorsplitting/cache) emit GoString calls on embedded values, and
// wiresmith does not generate GoString. Retire these once those importers
// migrate to wiresmith.

func (m *EncodedQueryTimeRange) GoString() string {
	if m == nil {
		return "nil"
	}
	return fmt.Sprintf("&%#v", *m)
}

func (m *EncodedOperatorEvaluationStats) GoString() string {
	if m == nil {
		return "nil"
	}
	return fmt.Sprintf("&%#v", *m)
}
