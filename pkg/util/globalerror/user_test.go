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

func TestID_MessageWithPerInstanceLimitConfig(t *testing.T) {
	for _, tc := range []struct {
		expected string
		actual   string
	}{
		{
			expected: "an error (err-mimir-missing-metric-name). To adjust the related limit, configure -my-flag1, or contact your service administrator.",
			actual:   MissingMetricName.MessageWithPerInstanceLimitConfig("an error", "my-flag1"),
		},
		{
			expected: "an error (err-mimir-missing-metric-name). To adjust the related limits, configure -my-flag1 and -my-flag2, or contact your service administrator.",
			actual:   MissingMetricName.MessageWithPerInstanceLimitConfig("an error", "my-flag1", "my-flag2"),
		},
		{
			expected: "an error (err-mimir-missing-metric-name). To adjust the related limits, configure -my-flag1, -my-flag2 and -my-flag3, or contact your service administrator.",
			actual:   MissingMetricName.MessageWithPerInstanceLimitConfig("an error", "my-flag1", "my-flag2", "my-flag3"),
		},
	} {
		assert.Equal(t, tc.expected, tc.actual)
	}
}

func TestID_MessageWithPerTenantLimitConfig(t *testing.T) {
	for _, tc := range []struct {
		expected string
		actual   string
	}{
		{
			expected: "an error (err-mimir-missing-metric-name). To adjust the related per-tenant limit, configure -my-flag1, or contact your service administrator.",
			actual:   MissingMetricName.MessageWithPerTenantLimitConfig("an error", "my-flag1"),
		},
		{
			expected: "an error (err-mimir-missing-metric-name). To adjust the related per-tenant limits, configure -my-flag1 and -my-flag2, or contact your service administrator.",
			actual:   MissingMetricName.MessageWithPerTenantLimitConfig("an error", "my-flag1", "my-flag2"),
		},
		{
			expected: "an error (err-mimir-missing-metric-name). To adjust the related per-tenant limits, configure -my-flag1, -my-flag2 and -my-flag3, or contact your service administrator.",
			actual:   MissingMetricName.MessageWithPerTenantLimitConfig("an error", "my-flag1", "my-flag2", "my-flag3"),
		},
	} {
		assert.Equal(t, tc.expected, tc.actual)
	}
}
