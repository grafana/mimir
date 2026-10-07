// SPDX-License-Identifier: AGPL-3.0-only

package readcacheassignment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupHealthyPods(t *testing.T) {
	view := GroupHealthyPods([]HealthyPod{
		{InstanceID: "readcache-zone-b-0", Zone: "zone-b", Addr: "10.0.0.2:9095"},
		{InstanceID: "readcache-zone-a-0", Addr: "10.0.0.1:9095"},
		{InstanceID: "readcache-zone-a-1", Zone: "zone-a", Addr: "10.0.0.3:9095"},
	})

	pods, ok := view.Pods("readcache-0")
	require.True(t, ok)
	assert.Equal(t, []SlotPod{
		{InstanceID: "readcache-zone-a-0", Zone: "zone-a", Addr: "10.0.0.1:9095"},
		{InstanceID: "readcache-zone-b-0", Zone: "zone-b", Addr: "10.0.0.2:9095"},
	}, pods, "zone is parsed from the name when the ring omits it, and pods are ordered by zone")

	_, ok = view.Pods("readcache-2")
	assert.False(t, ok, "a slot with no healthy pod is absent")

	addr, ok := view.Addr("readcache-zone-a-1")
	require.True(t, ok)
	assert.Equal(t, "10.0.0.3:9095", addr)
}

func TestSlotViewCache_TimestampOnlyUpdateIsNoop(t *testing.T) {
	cache := NewSlotViewCache()
	pods := []HealthyPod{
		{InstanceID: "readcache-zone-a-0", Zone: "zone-a", Addr: "10.0.0.1:9095"},
		{InstanceID: "readcache-zone-b-0", Zone: "zone-b", Addr: "10.0.0.2:9095"},
	}
	require.True(t, cache.Observe(pods))
	assert.False(t, cache.Observe([]HealthyPod{
		{InstanceID: "readcache-zone-b-0", Zone: "zone-b", Addr: "10.0.0.2:9095"},
		{InstanceID: "readcache-zone-a-0", Zone: "zone-a", Addr: "10.0.0.1:9095"},
	}), "order and heartbeat timestamps are not part of the healthy set")

	require.True(t, cache.Observe([]HealthyPod{
		{InstanceID: "readcache-zone-a-0", Zone: "zone-a", Addr: "10.0.0.9:9095"},
		{InstanceID: "readcache-zone-b-0", Zone: "zone-b", Addr: "10.0.0.2:9095"},
	}), "an address change rebuilds the view")
	addr, ok := cache.must()
	require.True(t, ok)
	assert.Equal(t, "10.0.0.9:9095", addr)
}

func TestSlotViewCache_Unavailable(t *testing.T) {
	cache := NewSlotViewCache()
	_, ok := cache.Current()
	assert.False(t, ok)

	require.True(t, cache.Observe([]HealthyPod{{InstanceID: "readcache-0", Addr: "10.0.0.1:9095"}}))
	_, ok = cache.Current()
	require.True(t, ok)

	cache.MarkUnavailable()
	_, ok = cache.Current()
	assert.False(t, ok, "a failed ring read drops the previous grouping")

	require.True(t, cache.Observe([]HealthyPod{{InstanceID: "readcache-0", Addr: "10.0.0.1:9095"}}),
		"the same pods after an outage are applied again")

	require.True(t, cache.Observe(nil), "a successful read of nobody healthy clears the slot")
	view, ok := cache.Current()
	require.True(t, ok)
	_, found := view.Pods("readcache-0")
	assert.False(t, found)
	assert.False(t, cache.Observe(nil), "a second empty read does not rebuild")
}

func TestReplicaMapIncluding(t *testing.T) {
	view := GroupHealthyPods([]HealthyPod{
		{InstanceID: "readcache-zone-a-0", Zone: "zone-a", Addr: "10.0.0.1:9095"},
	})
	m := view.ReplicaMapIncluding([]string{"readcache-0", "readcache-1"})
	require.Len(t, m["readcache-0"], 1)
	_, ok := m["readcache-1"]
	assert.True(t, ok)
	assert.Empty(t, m["readcache-1"])
}

func (c *SlotViewCache) must() (string, bool) {
	view, ok := c.Current()
	if !ok {
		return "", false
	}
	return view.Addr("readcache-zone-a-0")
}
