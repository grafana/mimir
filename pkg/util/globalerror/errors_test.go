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
	for _, tc := range []struct {
		expected string
		actual   string
	}{
		{
			expected: "an error (err-mimir-missing-metric-name). You can adjust the related per-tenant limit by configuring -my-flag1, or by contacting your service administrator.",
			actual:   MissingMetricName.MessageWithLimitConfig("an error", "my-flag1"),
		},
		{
			expected: "an error (err-mimir-missing-metric-name). You can adjust the related per-tenant limits by configuring -my-flag1 and -my-flag2, or by contacting your service administrator.",
			actual:   MissingMetricName.MessageWithLimitConfig("an error", "my-flag1", "my-flag2"),
		},
		{
			expected: "an error (err-mimir-missing-metric-name). You can adjust the related per-tenant limits by configuring -my-flag1, -my-flag2 and -my-flag3, or by contacting your service administrator.",
			actual:   MissingMetricName.MessageWithLimitConfig("an error", "my-flag1", "my-flag2", "my-flag3"),
		},
	} {
		assert.Equal(t, tc.expected, tc.actual)
	}
}
