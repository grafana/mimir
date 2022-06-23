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
	t.Run("all instances healthy and active", func(t *testing.T) {
		now := time.Now()
		heartbeatTimeout := time.Minute
		count := atomic.NewUint32(0)

		ringDesc := ring.NewDesc()
		instance1 := ringDesc.AddIngester("distributor-1", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, now)
		instance1.Timestamp = now.Unix()
		ringDesc.Ingesters["distributor-1"] = instance1

		instance2 := ringDesc.AddIngester("distributor-2", "127.0.0.2", "", []uint32{2}, ring.ACTIVE, now)
		instance2.Timestamp = now.Unix()
		ringDesc.Ingesters["distributor-2"] = instance2

		instance3 := ringDesc.AddIngester("distributor-3", "127.0.0.3", "", []uint32{3}, ring.ACTIVE, now)
		instance3.Timestamp = now.Unix()
		ringDesc.Ingesters["distributor-3"] = instance3

		delegate := newHealthyInstanceDelegate(count, heartbeatTimeout, &nopDelegate{})
		delegate.OnRingInstanceHeartbeat(&ring.BasicLifecycler{}, ringDesc, &instance1)

		assert.Equal(t, uint32(3), count.Load())
	})

	t.Run("all instances healthy not all instances active", func(t *testing.T) {
		now := time.Now()
		heartbeatTimeout := time.Minute
		count := atomic.NewUint32(0)

		ringDesc := ring.NewDesc()
		instance1 := ringDesc.AddIngester("distributor-1", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, now)
		instance1.Timestamp = now.Unix()
		ringDesc.Ingesters["distributor-1"] = instance1

		instance2 := ringDesc.AddIngester("distributor-2", "127.0.0.2", "", []uint32{2}, ring.LEAVING, now)
		instance2.Timestamp = now.Unix()
		ringDesc.Ingesters["distributor-2"] = instance2

		instance3 := ringDesc.AddIngester("distributor-3", "127.0.0.3", "", []uint32{3}, ring.ACTIVE, now)
		instance3.Timestamp = now.Unix()
		ringDesc.Ingesters["distributor-3"] = instance3

		delegate := newHealthyInstanceDelegate(count, heartbeatTimeout, &nopDelegate{})
		delegate.OnRingInstanceHeartbeat(&ring.BasicLifecycler{}, ringDesc, &instance1)

		assert.Equal(t, uint32(2), count.Load())
	})

	t.Run("some instances healthy all instances active", func(t *testing.T) {
		now := time.Now()
		heartbeatTimeout := time.Minute
		count := atomic.NewUint32(0)

		ringDesc := ring.NewDesc()
		instance1 := ringDesc.AddIngester("distributor-1", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, now)
		instance1.Timestamp = now.Unix()
		ringDesc.Ingesters["distributor-1"] = instance1

		instance2 := ringDesc.AddIngester("distributor-2", "127.0.0.2", "", []uint32{2}, ring.ACTIVE, now)
		instance2.Timestamp = now.Unix()
		ringDesc.Ingesters["distributor-2"] = instance2

		instance3 := ringDesc.AddIngester("distributor-3", "127.0.0.3", "", []uint32{3}, ring.ACTIVE, now)
		instance3.Timestamp = now.Add(-5 * heartbeatTimeout).Unix()
		ringDesc.Ingesters["distributor-3"] = instance3

		delegate := newHealthyInstanceDelegate(count, heartbeatTimeout, &nopDelegate{})
		delegate.OnRingInstanceHeartbeat(&ring.BasicLifecycler{}, ringDesc, &instance1)

		assert.Equal(t, uint32(2), count.Load())
	})

	t.Run("some instances healthy but timeout disabled all instances active", func(t *testing.T) {
		now := time.Now()
		heartbeatTimeout := time.Duration(0)
		count := atomic.NewUint32(0)

		ringDesc := ring.NewDesc()
		instance1 := ringDesc.AddIngester("distributor-1", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, now)
		instance1.Timestamp = now.Unix()
		ringDesc.Ingesters["distributor-1"] = instance1

		instance2 := ringDesc.AddIngester("distributor-2", "127.0.0.2", "", []uint32{2}, ring.ACTIVE, now)
		instance2.Timestamp = now.Unix()
		ringDesc.Ingesters["distributor-2"] = instance2

		instance3 := ringDesc.AddIngester("distributor-3", "127.0.0.3", "", []uint32{3}, ring.ACTIVE, now)
		instance3.Timestamp = now.Add(-5 * time.Minute).Unix()
		ringDesc.Ingesters["distributor-3"] = instance3

		delegate := newHealthyInstanceDelegate(count, heartbeatTimeout, &nopDelegate{})
		delegate.OnRingInstanceHeartbeat(&ring.BasicLifecycler{}, ringDesc, &instance1)

		assert.Equal(t, uint32(3), count.Load())
	})
}
