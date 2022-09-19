// SPDX-License-Identifier: AGPL-3.0-only

package schedulerdiscovery

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/require"
)

func TestRingServiceDiscovery(t *testing.T) {
	ctx := context.Background()
	cfg := Config{}
	flagext.DefaultValues(&cfg)

	// Check very frequently to speed up the test.
	cfg.SchedulerRing.RingCheckPeriod = 100 * time.Millisecond

	// Inject in-memory KV store.
	inmem, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })
	cfg.SchedulerRing.KVStore.Mock = inmem

	// Create an empty ring.
	require.NoError(t, inmem.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		return ring.NewDesc(), true, nil
	}))

	// Mock a receiver to keep track of all notified addresses.
	receiver := newNotificationsReceiverMock()

	sd, err := NewRingServiceDiscovery(cfg, "test", receiver, log.NewNopLogger(), nil)
	require.NoError(t, err)

	// Start the service discovery.
	require.NoError(t, services.StartAndAwaitRunning(ctx, sd))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, sd))
	})

	// Wait some time, we expect no address notified because the ring is empty.
	time.Sleep(time.Second)
	require.Empty(t, receiver.getDiscoveredAddresses())

	// Register some instances.
	require.NoError(t, inmem.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.AddIngester("instance-1", "instance-1", "", nil, ring.ACTIVE, time.Now())
		desc.AddIngester("instance-2", "instance-2", "", nil, ring.PENDING, time.Now())
		desc.AddIngester("instance-3", "instance-3", "", nil, ring.JOINING, time.Now())
		desc.AddIngester("instance-4", "instance-4", "", nil, ring.LEAVING, time.Now())
		return desc, true, nil
	}))

	test.Poll(t, time.Second, []string{"instance-1"}, func() interface{} {
		return receiver.getDiscoveredAddresses()
	})

	// Register more instances.
	require.NoError(t, inmem.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.AddIngester("instance-5", "instance-5", "", nil, ring.ACTIVE, time.Now())
		desc.AddIngester("instance-6", "instance-6", "", nil, ring.ACTIVE, time.Now())
		return desc, true, nil
	}))

	test.Poll(t, time.Second, []string{"instance-1", "instance-5", "instance-6"}, func() interface{} {
		return receiver.getDiscoveredAddresses()
	})

	// Unregister some instances.
	require.NoError(t, inmem.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.RemoveIngester("instance-1")
		desc.RemoveIngester("instance-6")
		return desc, true, nil
	}))

	test.Poll(t, time.Second, []string{"instance-5"}, func() interface{} {
		return receiver.getDiscoveredAddresses()
	})

	// A non-active instance switches to active.
	require.NoError(t, inmem.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		instance := desc.Ingesters["instance-2"]
		instance.State = ring.ACTIVE
		desc.Ingesters["instance-2"] = instance
		return desc, true, nil
	}))

	test.Poll(t, time.Second, []string{"instance-2", "instance-5"}, func() interface{} {
		return receiver.getDiscoveredAddresses()
	})

	// An active becomes unhealthy.
	require.NoError(t, inmem.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		instance := desc.Ingesters["instance-2"]
		instance.Timestamp = time.Now().Add(-2 * cfg.SchedulerRing.HeartbeatTimeout).Unix()
		desc.Ingesters["instance-2"] = instance
		return desc, true, nil
	}))

	test.Poll(t, time.Second, []string{"instance-5"}, func() interface{} {
		return receiver.getDiscoveredAddresses()
	})
}

type notificationsReceiverMock struct {
	discoveredAddressesMx sync.Mutex
	discoveredAddresses   map[string]struct{}
}

func newNotificationsReceiverMock() *notificationsReceiverMock {
	return &notificationsReceiverMock{
		discoveredAddresses: map[string]struct{}{},
	}
}

func (r *notificationsReceiverMock) AddressAdded(address string) {
	r.discoveredAddressesMx.Lock()
	defer r.discoveredAddressesMx.Unlock()

	r.discoveredAddresses[address] = struct{}{}
}

func (r *notificationsReceiverMock) AddressRemoved(address string) {
	r.discoveredAddressesMx.Lock()
	defer r.discoveredAddressesMx.Unlock()

	delete(r.discoveredAddresses, address)
}

func (r *notificationsReceiverMock) getDiscoveredAddresses() []string {
	r.discoveredAddressesMx.Lock()
	defer r.discoveredAddressesMx.Unlock()

	out := make([]string, 0, len(r.discoveredAddresses))
	for addr := range r.discoveredAddresses {
		out = append(out, addr)
	}
	sort.Strings(out)

	return out
}
