// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import "github.com/grafana/mimir/pkg/util/promqlext"

func init() {
	promqlext.ExtendPromQL()
}
