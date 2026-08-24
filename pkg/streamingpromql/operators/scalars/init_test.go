// SPDX-License-Identifier: AGPL-3.0-only

package scalars

import (
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	types.EnableManglingReturnedSlices.Store(true)
}
