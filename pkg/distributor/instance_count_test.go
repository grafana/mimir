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
	tests := map[string]struct {
		ringSetup        func(desc *ring.Desc) ring.InstanceDesc
		heartbeatTimeout time.Duration
		expectedCount    uint32
	}{
		"all instances healthy and active": {
			ringSetup: func(desc *ring.Desc) ring.InstanceDesc {
				now := time.Now()

				instance1 := desc.AddIngester("distributor-1", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, now)
				instance1.Timestamp = now.Unix()
				desc.Ingesters["distributor-1"] = instance1

				instance2 := desc.AddIngester("distributor-2", "127.0.0.2", "", []uint32{2}, ring.ACTIVE, now)
				instance2.Timestamp = now.Unix()
				desc.Ingesters["distributor-2"] = instance2

				instance3 := desc.AddIngester("distributor-3", "127.0.0.3", "", []uint32{3}, ring.ACTIVE, now)
				instance3.Timestamp = now.Unix()
				desc.Ingesters["distributor-3"] = instance3

				return instance1
			},
			heartbeatTimeout: time.Minute,
			expectedCount:    3,
		},

		"all instances healthy not all instances active": {
			ringSetup: func(desc *ring.Desc) ring.InstanceDesc {
				now := time.Now()

				instance1 := desc.AddIngester("distributor-1", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, now)
				instance1.Timestamp = now.Unix()
				desc.Ingesters["distributor-1"] = instance1

				instance2 := desc.AddIngester("distributor-2", "127.0.0.2", "", []uint32{2}, ring.LEAVING, now)
				instance2.Timestamp = now.Unix()
				desc.Ingesters["distributor-2"] = instance2

				instance3 := desc.AddIngester("distributor-3", "127.0.0.3", "", []uint32{3}, ring.ACTIVE, now)
				instance3.Timestamp = now.Unix()
				desc.Ingesters["distributor-3"] = instance3

				return instance1
			},
			heartbeatTimeout: time.Minute,
			expectedCount:    2,
		},

		"some instances healthy all instances active": {
			ringSetup: func(desc *ring.Desc) ring.InstanceDesc {
				now := time.Now()

				instance1 := desc.AddIngester("distributor-1", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, now)
				instance1.Timestamp = now.Unix()
				desc.Ingesters["distributor-1"] = instance1

				instance2 := desc.AddIngester("distributor-2", "127.0.0.2", "", []uint32{2}, ring.ACTIVE, now)
				instance2.Timestamp = now.Unix()
				desc.Ingesters["distributor-2"] = instance2

				instance3 := desc.AddIngester("distributor-3", "127.0.0.3", "", []uint32{3}, ring.ACTIVE, now)
				instance3.Timestamp = now.Add(-5 * time.Minute).Unix()
				desc.Ingesters["distributor-3"] = instance3

				return instance1
			},
			heartbeatTimeout: time.Minute,
			expectedCount:    2,
		},

		"some instances healthy but timeout disabled all instances active": {
			ringSetup: func(desc *ring.Desc) ring.InstanceDesc {
				now := time.Now()

				instance1 := desc.AddIngester("distributor-1", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, now)
				instance1.Timestamp = now.Unix()
				desc.Ingesters["distributor-1"] = instance1

				instance2 := desc.AddIngester("distributor-2", "127.0.0.2", "", []uint32{2}, ring.ACTIVE, now)
				instance2.Timestamp = now.Unix()
				desc.Ingesters["distributor-2"] = instance2

				instance3 := desc.AddIngester("distributor-3", "127.0.0.3", "", []uint32{3}, ring.ACTIVE, now)
				instance3.Timestamp = now.Add(-5 * time.Minute).Unix()
				desc.Ingesters["distributor-3"] = instance3

				return instance1
			},
			heartbeatTimeout: 0,
			expectedCount:    3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			count := atomic.NewUint32(0)
			ringDesc := ring.NewDesc()

			firstInstance := testData.ringSetup(ringDesc)
			delegate := newHealthyInstanceDelegate(count, testData.heartbeatTimeout, &nopDelegate{})
			delegate.OnRingInstanceHeartbeat(&ring.BasicLifecycler{}, ringDesc, &firstInstance)

			assert.Equal(t, testData.expectedCount, count.Load())
		})
	}
}
