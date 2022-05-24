// SPDX-License-Identifier: AGPL-3.0-only

package globalerror

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestID_Message(t *testing.T) {
	assert.Equal(
		t,
		"an error (err-mimir-missing-metric-name)",
		MissingMetricName.Message("an error"))
}

func TestID_MessageWithLimitConfig(t *testing.T) {
	assert.Equal(
		t,
		"an error (err-mimir-missing-metric-name). You can adjust the related per-tenant limit by configuring -my-flag, or by contacting your service administrator.",
		MissingMetricName.MessageWithLimitConfig("my-flag", "an error"))
}
