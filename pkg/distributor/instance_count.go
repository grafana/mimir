// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"time"

	"github.com/grafana/dskit/ring"
	"go.uber.org/atomic"
)

// healthyInstanceDelegate counts the number of healthy instances that are part of the ring
// and stores the count to the provided atomic integer. Used here to count the number of
// distributors in the ring to determine how to enforce rate limiting.
//
// If the ring is zone-aware healthyInstanceDelegate counts the number of healthy
// instances in the current instance's zone and the number of ring zones.
type healthyInstanceDelegate struct {
	healthyInstancesCount       *atomic.Uint32
	healthyInstancesInZoneCount *atomic.Uint32
	ringZonesCount              *atomic.Uint32

	heartbeatTimeout time.Duration
	next             ring.BasicLifecyclerDelegate
}

func newHealthyInstanceDelegate(
	instanceCount, instanceInZoneCount, zoneCount *atomic.Uint32,
	heartbeatTimeout time.Duration,
	next ring.BasicLifecyclerDelegate,
) *healthyInstanceDelegate {
	return &healthyInstanceDelegate{
		healthyInstancesCount:       instanceCount,
		healthyInstancesInZoneCount: instanceInZoneCount,
		ringZonesCount:              zoneCount,
		heartbeatTimeout:            heartbeatTimeout,
		next:                        next,
	}
}

// OnRingInstanceRegister implements the ring.BasicLifecyclerDelegate interface
func (d *healthyInstanceDelegate) OnRingInstanceRegister(lifecycler *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	return d.next.OnRingInstanceRegister(lifecycler, ringDesc, instanceExists, instanceID, instanceDesc)
}

// OnRingInstanceTokens implements the ring.BasicLifecyclerDelegate interface
func (d *healthyInstanceDelegate) OnRingInstanceTokens(lifecycler *ring.BasicLifecycler, tokens ring.Tokens) {
	d.next.OnRingInstanceTokens(lifecycler, tokens)
}

// OnRingInstanceStopping implements the ring.BasicLifecyclerDelegate interface
func (d *healthyInstanceDelegate) OnRingInstanceStopping(lifecycler *ring.BasicLifecycler) {
	d.next.OnRingInstanceStopping(lifecycler)
}

// OnRingInstanceHeartbeat implements the ring.BasicLifecyclerDelegate interface
func (d *healthyInstanceDelegate) OnRingInstanceHeartbeat(lifecycler *ring.BasicLifecycler, ringDesc *ring.Desc, instanceDesc *ring.InstanceDesc) {
	activeMembers := uint32(0)
	activeMembersInZone := uint32(0)
	zoneCount := uint32(1)
	now := time.Now()

	zone := lifecycler.GetInstanceZone()
	zones := make(map[string]struct{}, 5)
	zones[zone] = struct{}{}

	for _, instance := range ringDesc.Ingesters {
		if ring.ACTIVE == instance.State && instance.IsHeartbeatHealthy(d.heartbeatTimeout, now) {
			activeMembers++
			if zone != "" && instance.Zone == zone {
				activeMembersInZone++
			}
		}
		if zone != "" {
			if _, ok := zones[instance.Zone]; !ok {
				zones[instance.Zone] = struct{}{}
				zoneCount++
			}
		}
	}

	d.ringZonesCount.Store(zoneCount)
	d.healthyInstancesCount.Store(activeMembers)
	d.healthyInstancesInZoneCount.Store(activeMembersInZone)
	d.next.OnRingInstanceHeartbeat(lifecycler, ringDesc, instanceDesc)
}
