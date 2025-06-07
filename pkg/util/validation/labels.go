// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"github.com/prometheus/common/model"
)

// IsValidLabelName returns true iff name matches prometheus
// "LegacyValidation" name validation scheme.
func IsValidLabelName(name string) bool {
	return model.LabelName(name).IsValidLegacy()
}

// IsValidMetricName returns true iff the name matches prometheus
// "LegacyValidation" name validation scheme.
func IsValidMetricName(name string) bool {
	return model.IsValidLegacyMetricName(name)
}
