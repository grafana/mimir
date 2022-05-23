// SPDX-License-Identifier: AGPL-3.0-only

package globalerrors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormat(t *testing.T) {
	assert.Equal(t, "an error with value 123 (err-mimir-missing-metric-name)", Format(ErrIDMissingMetricName, "an error with value %d", 123))
}

func TestFormatWithLimitConfig(t *testing.T) {
	assert.Equal(t, "an error with value 123 (err-mimir-missing-metric-name). You can adjust the related per-tenant limit by configuring -my-flag, or by contacting your service administrator.", FormatWithLimitConfig(ErrIDMissingMetricName, "my-flag", "an error with value %d", 123))
}
