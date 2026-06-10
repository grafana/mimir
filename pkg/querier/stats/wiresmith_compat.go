// SPDX-License-Identifier: AGPL-3.0-only

package stats

import (
	"fmt"
)

// GoString preserves the gogoslick-generated API: still-gogo-compiled
// importers of stats.proto (e.g. pkg/querier/querierpb) emit GoString calls
// on embedded Stats values, and wiresmith does not generate GoString.
func (m *Stats) GoString() string {
	if m == nil {
		return "nil"
	}
	return fmt.Sprintf("&%#v", *m)
}
