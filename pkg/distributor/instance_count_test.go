// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

type nopDelegate struct{}

func (n nopDelegate) OnRingInstanceRegister(_ *ring.BasicLifecycler, _ ring.Desc, _ bool, _ string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	return instanceDesc.State, instanceDesc.GetTokens()
}

func (n nopDelegate) OnRingInstanceTokens(*ring.BasicLifecycler, ring.Tokens) {
}

func (n nopDelegate) OnRingInstanceStopping(*ring.BasicLifecycler) {
}

func (n nopDelegate) OnRingInstanceHeartbeat(*ring.BasicLifecycler, *ring.Desc, *ring.InstanceDesc) {
}

func TestHealthyInstanceDelegate_OnRingInstanceHeartbeat(t *testing.T) {
	// addInstance registers a new instance with the given ring and sets its last heartbeat timestamp
	addInstance := func(desc *ring.Desc, id, zone string, state ring.InstanceState, timestamp int64) {
		instance := desc.AddIngester(id, "127.0.0.1", zone, []uint32{1}, state, time.Now(), false, time.Time{}, nil)
		instance.Timestamp = timestamp
		desc.Ingesters[id] = instance
	}

	tests := map[string]struct {
		ringSetup         func(desc *ring.Desc)
		zone              string
		expectedInstances uint32
		expectedInZone    uint32
		expectedRingZones uint32
	}{
		"all instances healthy and active": {
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				addInstance(desc, "distributor-1", "", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-2", "", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-3", "", ring.ACTIVE, now.Unix())
			},
			expectedInstances: 3,
			expectedRingZones: 1,
		},
		"all instances healthy not all instances active": {
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				addInstance(desc, "distributor-1", "", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-2", "", ring.LEAVING, now.Unix())
				addInstance(desc, "distributor-3", "", ring.ACTIVE, now.Unix())
			},
			expectedInstances: 2,
			expectedRingZones: 1,
		},
		"some instances healthy all instances active": {
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				addInstance(desc, "distributor-1", "", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-2", "", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-3", "", ring.ACTIVE, now.Add(-5*time.Minute).Unix())
			},
			expectedInstances: 2,
			expectedRingZones: 1,
		},
		"all instances healthy across multiple zones": {
			zone: "zone-a",
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				addInstance(desc, "distributor-1", "zone-a", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-2", "zone-b", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-3", "zone-c", ring.ACTIVE, now.Unix())
			},
			expectedInstances: 3,
			expectedRingZones: 3,
			expectedInZone:    1,
		},
		"all instances healthy across multiple zones, but ring not configured to use zone": {
			zone: "",
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				addInstance(desc, "distributor-1", "zone-a", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-2", "zone-b", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-3", "zone-c", ring.ACTIVE, now.Unix())
			},
			expectedInstances: 3,
			expectedRingZones: 1,
		},
		"uneven distribution across 2 zones": {
			zone: "zone-a",
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				for i := 0; i < 35; i++ {
					addInstance(desc, fmt.Sprintf("distributor-a-%d", i), "zone-a", ring.ACTIVE, now.Unix())
				}
				for i := 0; i < 5; i++ {
					addInstance(desc, fmt.Sprintf("distributor-b-%d", i), "zone-b", ring.ACTIVE, now.Unix())
				}
			},
			expectedInstances: 35 + 5,
			expectedInZone:    35,
			expectedRingZones: 2,
		},
		"uneven distribution across 2 zones, counting from smaller zone": {
			zone: "zone-b",
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				for i := 0; i < 35; i++ {
					addInstance(desc, fmt.Sprintf("distributor-a-%d", i), "zone-a", ring.ACTIVE, now.Unix())
				}
				for i := 0; i < 5; i++ {
					addInstance(desc, fmt.Sprintf("distributor-b-%d", i), "zone-b", ring.ACTIVE, now.Unix())
				}
			},
			expectedInstances: 35 + 5,
			expectedInZone:    5,
			expectedRingZones: 2,
		},
		"zone-aware with some unhealthy in own zone": {
			zone: "zone-a",
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				addInstance(desc, "distributor-a-1", "zone-a", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-a-2", "zone-a", ring.ACTIVE, now.Add(-5*time.Minute).Unix())
				addInstance(desc, "distributor-a-3", "zone-a", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-b-1", "zone-b", ring.ACTIVE, now.Unix())
			},
			expectedInstances: 3,
			expectedInZone:    2,
			expectedRingZones: 2,
		},
		"zone-aware with non-active instances in own zone": {
			zone: "zone-a",
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				addInstance(desc, "distributor-a-1", "zone-a", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-a-2", "zone-a", ring.LEAVING, now.Unix())
				addInstance(desc, "distributor-b-1", "zone-b", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-b-2", "zone-b", ring.ACTIVE, now.Unix())
			},
			expectedInstances: 3,
			expectedInZone:    1,
			expectedRingZones: 2,
		},
		"zone-aware with unhealthy instances in other zone": {
			zone: "zone-a",
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				addInstance(desc, "distributor-a-1", "zone-a", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-a-2", "zone-a", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-b-1", "zone-b", ring.ACTIVE, now.Add(-5*time.Minute).Unix())
			},
			expectedInstances: 2,
			expectedInZone:    2,
			expectedRingZones: 2,
		},
		"zone-aware single zone": {
			zone: "zone-a",
			ringSetup: func(desc *ring.Desc) {
				now := time.Now()
				addInstance(desc, "distributor-1", "zone-a", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-2", "zone-a", ring.ACTIVE, now.Unix())
				addInstance(desc, "distributor-3", "zone-a", ring.ACTIVE, now.Unix())
			},
			expectedInstances: 3,
			expectedInZone:    3,
			expectedRingZones: 1,
		},
		"zone-aware empty ring": {
			zone: "zone-a",
			ringSetup: func(desc *ring.Desc) {
			},
			expectedInstances: 0,
			expectedRingZones: 1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			instanceCount := atomic.NewUint32(0)
			instanceInZoneCount := atomic.NewUint32(0)
			zoneCount := atomic.NewUint32(0)
			ringDesc := ring.NewDesc()
			lifecycler, _ := ring.NewBasicLifecycler(ring.BasicLifecyclerConfig{
				Zone: testData.zone,
			}, "", "", nil, nil, nil, nil)

			testData.ringSetup(ringDesc)
			instance := ringDesc.Ingesters["distributor-1"]

			delegate := newHealthyInstanceDelegate(instanceCount, instanceInZoneCount, zoneCount, time.Minute, &nopDelegate{})
			delegate.OnRingInstanceHeartbeat(lifecycler, ringDesc, &instance)

			assert.Equal(t, int(testData.expectedInstances), int(instanceCount.Load()))
			assert.Equal(t, int(testData.expectedInZone), int(instanceInZoneCount.Load()))
			assert.Equal(t, int(testData.expectedRingZones), int(zoneCount.Load()))
		})
	}
}
