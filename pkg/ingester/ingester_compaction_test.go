// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompactionLoopExtraDelay(t *testing.T) {
	const (
		standardInterval = time.Minute
		minDelay         = 10 * time.Second
	)

	tests := map[string]struct {
		iterationDuration time.Duration
		expected          time.Duration
	}{
		"fast iteration well below interval: no extra delay": {
			iterationDuration: 5 * time.Second,
			expected:          0,
		},
		"iteration exactly at the boundary: no extra delay": {
			iterationDuration: standardInterval - minDelay,
			expected:          0,
		},
		"iteration just past the boundary: small extra delay": {
			iterationDuration: standardInterval - minDelay + time.Second,
			expected:          time.Second,
		},
		"iteration near the full interval: extra delay close to minDelay": {
			iterationDuration: standardInterval - time.Second,
			expected:          minDelay - time.Second,
		},
		"iteration exactly at the interval: extra delay equals minDelay": {
			iterationDuration: standardInterval,
			expected:          minDelay,
		},
		"iteration overruns the interval: extra delay capped at minDelay": {
			iterationDuration: standardInterval + time.Second,
			expected:          minDelay,
		},
		"iteration far overruns the interval: extra delay still capped at minDelay": {
			iterationDuration: 10 * standardInterval,
			expected:          minDelay,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := compactionLoopExtraDelay(tc.iterationDuration, standardInterval, minDelay)
			assert.Equal(t, tc.expected, got)
		})
	}
}
