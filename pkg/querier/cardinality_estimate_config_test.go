// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCardinalityEstimateConfig_Validate(t *testing.T) {
	for name, tc := range map[string]struct {
		trust, ignoreWithin time.Duration
		wantErr             bool
	}{
		"trust off":                        {trust: 0, ignoreWithin: 10 * time.Hour},
		"trust shorter than ignore-within": {trust: time.Hour, ignoreWithin: 3 * time.Hour, wantErr: true},
		"trust equal to ignore-within":     {trust: 3 * time.Hour, ignoreWithin: 3 * time.Hour},
		"trust longer than ignore-within":  {trust: 12 * time.Hour, ignoreWithin: 10 * time.Hour},
		"ignore-within off":                {trust: time.Hour, ignoreWithin: 0},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := CardinalityEstimateConfig{TrustCompactedBlocksAfter: tc.trust}
			err := cfg.Validate(tc.ignoreWithin)
			if tc.wantErr {
				assert.ErrorIs(t, err, errTrustCompactedBlocksBeforeIgnored)
				return
			}
			assert.NoError(t, err)
		})
	}

	t.Run("a negative max-groups is refused at startup", func(t *testing.T) {
		cfg := CardinalityEstimateConfig{MaxGroups: -1}
		assert.ErrorIs(t, cfg.Validate(0), errNegativeMaxGroups)
		cfg.MaxGroups = 0
		assert.NoError(t, cfg.Validate(0))
	})
}
