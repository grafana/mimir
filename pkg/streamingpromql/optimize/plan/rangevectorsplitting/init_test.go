// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting_test

import (
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	types.EnableManglingReturnedSlices = true
}
