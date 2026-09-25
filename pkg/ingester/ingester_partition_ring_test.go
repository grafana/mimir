// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
)

// The ingester flag for derived tokens only takes effect through this copy. The integration test covers
// the whole path, from the flag to a partition with derived tokens.
func TestPartitionRingConfig_ToLifecyclerConfig(t *testing.T) {
	cfg := PartitionRingConfig{
		MinOwnersCount:                    2,
		MinOwnersDuration:                 time.Minute,
		DeleteInactivePartitionAfter:      time.Hour,
		CreatePartitionsWithDerivedTokens: true,
		MaxDerivedTokenPartitions:         128,
		lifecyclerPollingInterval:         time.Second,
	}

	expected := ring.PartitionInstanceLifecyclerConfig{
		PartitionID:                          3,
		InstanceID:                           "ingester-zone-a-3",
		WaitOwnersCountOnPending:             2,
		WaitOwnersDurationOnPending:          time.Minute,
		DeleteInactivePartitionAfterDuration: time.Hour,
		PollingInterval:                      time.Second,
		CreatePartitionsWithDerivedTokens:    true,
		MaxDerivedTokenPartitions:            128,
	}
	assert.Equal(t, expected, cfg.ToLifecyclerConfig(3, "ingester-zone-a-3"))
}
