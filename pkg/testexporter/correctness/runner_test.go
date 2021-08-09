// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/testexporter/correctness/runner_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package correctness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMinQueryTime(t *testing.T) {
	tests := []struct {
		durationQuerySince time.Duration
		timeQueryStart     TimeValue
		expected           time.Time
	}{
		{
			expected: time.Now(),
		},
		{
			timeQueryStart: NewTimeValue(time.Unix(1234567890, 0)),
			expected:       time.Unix(1234567890, 0),
		},
		{
			durationQuerySince: 10 * time.Hour,
			expected:           time.Now().Add(-10 * time.Hour),
		},
	}

	for _, tt := range tests {
		assert.WithinDuration(t, tt.expected, calculateMinQueryTime(tt.durationQuerySince, tt.timeQueryStart), 50*time.Millisecond)
	}
}
