// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/storage/caching_fixtures.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storage

import (
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/util/validation"
)

func defaultLimits() (*validation.Overrides, error) {
	var defaults validation.Limits
	flagext.DefaultValues(&defaults)
	defaults.CardinalityLimit = 5
	return validation.NewOverrides(defaults, nil)
}
