// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"github.com/grafana/dskit/ring"
	"go.uber.org/atomic"
)

// healthyInstanceDelegate counts the number of healthy instances that are part of the ring
// and stores the count to the provided atomic integer. Used here to count the number of
// distributors in the ring to determine how to enforce rate limiting.
type healthyInstanceDelegate struct {
	count *atomic.Uint32
	next  ring.BasicLifecyclerDelegate
}

func newHealthyInstanceDelegate(count *atomic.Uint32, next ring.BasicLifecyclerDelegate) *healthyInstanceDelegate {
	return &healthyInstanceDelegate{count: count, next: next}
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

	for _, instance := range ringDesc.Ingesters {
		if ring.ACTIVE == instance.State {
			activeMembers++
		}
	}

	d.count.Store(activeMembers)
	d.next.OnRingInstanceHeartbeat(lifecycler, ringDesc, instanceDesc)
}
