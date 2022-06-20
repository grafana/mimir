// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

type nopDelegate struct{}

func (n nopDelegate) OnRingInstanceRegister(lifecycler *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	return instanceDesc.State, instanceDesc.GetTokens()
}

func (n nopDelegate) OnRingInstanceTokens(lifecycler *ring.BasicLifecycler, tokens ring.Tokens) {
}

func (n nopDelegate) OnRingInstanceStopping(lifecycler *ring.BasicLifecycler) {
}

func (n nopDelegate) OnRingInstanceHeartbeat(lifecycler *ring.BasicLifecycler, ringDesc *ring.Desc, instanceDesc *ring.InstanceDesc) {
}

func TestHealthyInstanceDelegate_OnRingInstanceHeartbeat(t *testing.T) {
	t.Run("all instances healthy", func(t *testing.T) {
		now := time.Now()
		count := atomic.NewUint32(0)

		ringDesc := ring.NewDesc()
		ringDesc.AddIngester("distributor-1", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, now)
		ringDesc.AddIngester("distributor-2", "127.0.0.2", "", []uint32{2}, ring.ACTIVE, now)
		ringDesc.AddIngester("distributor-3", "127.0.0.3", "", []uint32{3}, ring.ACTIVE, now)
		instance := ringDesc.Ingesters["distributor-1"]

		delegate := newHealthyInstanceDelegate(count, &nopDelegate{})
		delegate.OnRingInstanceHeartbeat(&ring.BasicLifecycler{}, ringDesc, &instance)

		assert.Equal(t, uint32(3), count.Load())
	})

	t.Run("some instances not healthy", func(t *testing.T) {
		now := time.Now()
		count := atomic.NewUint32(0)

		ringDesc := ring.NewDesc()
		ringDesc.AddIngester("distributor-1", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, now)
		ringDesc.AddIngester("distributor-2", "127.0.0.2", "", []uint32{2}, ring.LEAVING, now)
		ringDesc.AddIngester("distributor-3", "127.0.0.3", "", []uint32{3}, ring.JOINING, now)
		instance := ringDesc.Ingesters["distributor-1"]

		delegate := newHealthyInstanceDelegate(count, &nopDelegate{})
		delegate.OnRingInstanceHeartbeat(&ring.BasicLifecycler{}, ringDesc, &instance)

		assert.Equal(t, uint32(1), count.Load())
	})
}
