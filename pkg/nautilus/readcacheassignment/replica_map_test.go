// SPDX-License-Identifier: AGPL-3.0-only

package readcacheassignment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseInstanceIdentity(t *testing.T) {
	t.Run("zonal", func(t *testing.T) {
		id, ok := ParseInstanceIdentity("readcache-zone-a-5")
		require.True(t, ok)
		assert.Equal(t, "readcache-5", id.LogicalID)
		assert.Equal(t, "zone-a", id.Zone)
		assert.Equal(t, 5, id.Ordinal)
	})
	t.Run("zonal zone-b", func(t *testing.T) {
		id, ok := ParseInstanceIdentity("readcache-zone-b-12")
		require.True(t, ok)
		assert.Equal(t, "readcache-12", id.LogicalID)
		assert.Equal(t, "zone-b", id.Zone)
		assert.Equal(t, 12, id.Ordinal)
	})
	t.Run("legacy non-zonal", func(t *testing.T) {
		id, ok := ParseInstanceIdentity("readcache-5")
		require.True(t, ok)
		assert.Equal(t, "readcache-5", id.LogicalID)
		assert.Equal(t, "", id.Zone)
		assert.Equal(t, 5, id.Ordinal)
	})
	t.Run("unparseable", func(t *testing.T) {
		_, ok := ParseInstanceIdentity("readcache")
		assert.False(t, ok)
	})
}

func TestReplicaMapExpandIdentity(t *testing.T) {
	var m ReplicaMap
	assert.Equal(t, []Replica{{InstanceID: "readcache-5"}}, m.Expand("readcache-5"))
	assert.True(t, m.OwnsLogical("readcache-5", "readcache-5"))
	assert.False(t, m.OwnsLogical("readcache-zone-a-5", "readcache-5"))
}

func TestReplicaMapExpandRF2(t *testing.T) {
	m := ReplicaMap{
		"readcache-5": {
			{InstanceID: "readcache-zone-a-5", Zone: "zone-a"},
			{InstanceID: "readcache-zone-b-5", Zone: "zone-b"},
		},
	}
	assert.Equal(t, []string{"readcache-zone-a-5", "readcache-zone-b-5"}, m.ConcreteIDs("readcache-5"))
	assert.True(t, m.OwnsLogical("readcache-zone-a-5", "readcache-5"))
	assert.True(t, m.OwnsLogical("readcache-zone-b-5", "readcache-5"))
	assert.False(t, m.OwnsLogical("readcache-zone-a-6", "readcache-5"))

	logical, ok := m.LogicalForConcrete("readcache-zone-b-5")
	require.True(t, ok)
	assert.Equal(t, "readcache-5", logical)
}

func TestBuildReplicaMap(t *testing.T) {
	m := BuildReplicaMap([]Replica{
		{InstanceID: "readcache-zone-b-1", Zone: "zone-b"},
		{InstanceID: "readcache-zone-a-1", Zone: "zone-a"},
		{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
		{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
	})
	require.Equal(t, []string{"readcache-0", "readcache-1"}, m.SortedLogicalIDs())
	assert.Equal(t, []Replica{
		{InstanceID: "readcache-zone-a-0", Zone: "zone-a"},
		{InstanceID: "readcache-zone-b-0", Zone: "zone-b"},
	}, m["readcache-0"])
	assert.Equal(t, []Replica{
		{InstanceID: "readcache-zone-a-1", Zone: "zone-a"},
		{InstanceID: "readcache-zone-b-1", Zone: "zone-b"},
	}, m["readcache-1"])
}

func TestDesiredLogicalSlots(t *testing.T) {
	assert.Nil(t, DesiredLogicalSlots("readcache", 0))
	assert.Equal(t, []string{"readcache-0", "readcache-1", "readcache-2"}, DesiredLogicalSlots("readcache", 3))
}
