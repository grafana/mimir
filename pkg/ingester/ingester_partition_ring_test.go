// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestPartitionRingConfig_ToLifecyclerConfig(t *testing.T) {
	cfg := PartitionRingConfig{
		MinOwnersCount:                    2,
		MinOwnersDuration:                 time.Minute,
		DeleteInactivePartitionAfter:      10 * time.Minute,
		CreatePartitionsWithDerivedTokens: true,
		MaxDerivedTokenPartitions:         128,
		lifecyclerPollingInterval:         time.Second,
	}

	limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
		defaults.QueryIngestersWithin = model.Duration(time.Hour)
	})

	lifecyclerCfg := cfg.ToLifecyclerConfig(3, "ingester-zone-a-3", limits, 5*time.Minute)

	assert.Equal(t, int32(3), lifecyclerCfg.PartitionID)
	assert.Equal(t, "ingester-zone-a-3", lifecyclerCfg.InstanceID)
	assert.Equal(t, 2, lifecyclerCfg.WaitOwnersCountOnPending)
	assert.Equal(t, time.Minute, lifecyclerCfg.WaitOwnersDurationOnPending)
	assert.Equal(t, time.Second, lifecyclerCfg.PollingInterval)
	assert.True(t, lifecyclerCfg.CreatePartitionsWithDerivedTokens)
	assert.Equal(t, int32(128), lifecyclerCfg.MaxDerivedTokenPartitions)
	assert.Zero(t, lifecyclerCfg.DeleteInactivePartitionAfterDuration)

	require.NotNil(t, lifecyclerCfg.DeleteInactivePartitionAfterDurationFunc)
	d, err := lifecyclerCfg.DeleteInactivePartitionAfterDurationFunc()
	require.NoError(t, err)
	assert.Equal(t, time.Hour+5*time.Minute+10*time.Minute, d)
}

func TestPartitionRingConfig_DeleteInactivePartitionAfter(t *testing.T) {
	tests := map[string]struct {
		buffer        time.Duration
		defaultWithin time.Duration
		tenantWithin  map[string]time.Duration
		offset        time.Duration
		expected      time.Duration
	}{
		"default only": {
			buffer: 10 * time.Minute, defaultWithin: 13 * time.Hour, offset: 10 * time.Minute,
			expected: 13*time.Hour + 20*time.Minute,
		},
		"tenant override larger than default": {
			buffer: 10 * time.Minute, defaultWithin: 13 * time.Hour, offset: 10 * time.Minute,
			tenantWithin: map[string]time.Duration{"a": 24 * time.Hour, "b": time.Hour},
			expected:     24*time.Hour + 20*time.Minute,
		},
		"tenant override smaller than default": {
			buffer: 10 * time.Minute, defaultWithin: 13 * time.Hour, offset: 10 * time.Minute,
			tenantWithin: map[string]time.Duration{"a": time.Hour},
			expected:     13*time.Hour + 20*time.Minute,
		},
		"default query-ingesters-within is 0": {
			buffer: 10 * time.Minute, defaultWithin: 0, offset: 10 * time.Minute,
			tenantWithin: map[string]time.Duration{"a": time.Hour},
			expected:     0,
		},
		"tenant query-ingesters-within is 0": {
			buffer: 10 * time.Minute, defaultWithin: 13 * time.Hour, offset: 10 * time.Minute,
			tenantWithin: map[string]time.Duration{"a": 0},
			expected:     0,
		},
		"offset duration is 0": {
			buffer: 10 * time.Minute, defaultWithin: 13 * time.Hour, offset: 0,
			expected: 0,
		},
		"offset duration is negative": {
			buffer: 10 * time.Minute, defaultWithin: 13 * time.Hour, offset: -time.Second,
			expected: 0,
		},
		"buffer is 0 disables deletion": {
			buffer: 0, defaultWithin: 13 * time.Hour, offset: 10 * time.Minute,
			expected: 0,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			limits := validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
				defaults.QueryIngestersWithin = model.Duration(tc.defaultWithin)
				for tenantID, within := range tc.tenantWithin {
					l := *defaults
					l.QueryIngestersWithin = model.Duration(within)
					tenantLimits[tenantID] = &l
				}
			})

			cfg := PartitionRingConfig{DeleteInactivePartitionAfter: tc.buffer}
			assert.Equal(t, tc.expected, cfg.deleteInactivePartitionAfter(limits, tc.offset))
		})
	}
}
