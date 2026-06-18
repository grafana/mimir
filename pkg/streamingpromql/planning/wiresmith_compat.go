// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"
)

// GoString preserves the gogoslick-generated API: the still-gogo-compiled
// pkg/querier/querierpb importer emits a GoString call on an embedded
// EncodedQueryPlan value, and wiresmith does not generate GoString. Retire this
// once querierpb migrates to wiresmith.
func (m *EncodedQueryPlan) GoString() string {
	if m == nil {
		return "nil"
	}
	return fmt.Sprintf("&%#v", *m)
}
