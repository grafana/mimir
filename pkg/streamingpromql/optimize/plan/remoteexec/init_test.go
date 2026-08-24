// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec_test

import (
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	types.EnableManglingReturnedSlices.Store(true)
}
