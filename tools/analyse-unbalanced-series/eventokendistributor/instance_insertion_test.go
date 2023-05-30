package eventokendistributor

import (
	"container/heap"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"
)

func TestSpreadMinimizingRing_CreateTokenOwnershipPriorityQueues(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 2
	tokensPerInstance := 4
	tokensByInstanceID := createTokensByInstanceID(instancesPerZone, tokensPerInstance, zones)
	ringDesc := createRigDesc(instancesPerZone, zones, tokensByInstanceID)
	r := NewSpreadMinimizingRing(ringDesc, zones, tokensPerInstance, math.MaxUint32)
	ownershipByToken := map[uint32]float64{
		20:  50,
		50:  30,
		150: 100,
		210: 60,
		500: 290,
		650: 150,
		880: 230,
		970: 90,
	}

	instancesByToken := map[uint32]InstanceInfo{
		20:  {InstanceID: "instance-0", Zone: "zone-a"},
		50:  {InstanceID: "instance-1", Zone: "zone-a"},
		150: {InstanceID: "instance-0", Zone: "zone-a"},
		210: {InstanceID: "instance-1", Zone: "zone-a"},
		500: {InstanceID: "instance-0", Zone: "zone-a"},
		650: {InstanceID: "instance-1", Zone: "zone-a"},
		880: {InstanceID: "instance-1", Zone: "zone-a"},
		970: {InstanceID: "instance-0", Zone: "zone-a"},
	}

	queues := r.createTokenOwnershipPriorityQueues(instancesPerZone, ownershipByToken, instancesByToken)
	queueInstance0, ok := queues["instance-0"]
	require.True(t, ok)
	require.Equal(t, tokensPerInstance, queueInstance0.Len())
	oi := heap.Pop(queueInstance0).(*OwnershipInfo)
	require.Equal(t, float64(290), oi.ownership)
	rt := oi.ringItem.(*RingToken)
	require.Equal(t, uint32(500), rt.token)

	queueInstance1, ok := queues["instance-1"]
	require.True(t, ok)
	require.Equal(t, tokensPerInstance, queueInstance1.Len())
	oi = heap.Pop(queueInstance1).(*OwnershipInfo)
	require.Equal(t, float64(230), oi.ownership)
	rt = oi.ringItem.(*RingToken)
	require.Equal(t, uint32(880), rt.token)
}

func TestSpreadMinimizingRing_CreateInstanceOwnershipPriorityQueue(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 2
	tokensPerInstance := 4
	tokensByInstanceID := createTokensByInstanceID(instancesPerZone, tokensPerInstance, zones)
	ringDesc := createRigDesc(instancesPerZone, zones, tokensByInstanceID)
	r := NewSpreadMinimizingRing(ringDesc, zones, tokensPerInstance, math.MaxUint32)
	ownershipByInstance := map[string]float64{
		"instance-0": 950,
		"instance-1": 1300,
		"instance-2": 300,
	}

	queue := r.createInstanceOwnershipPriorityQueue(ownershipByInstance)
	require.NotNil(t, queue)
	require.Equal(t, 3, queue.Len())

	oi := heap.Pop(queue).(*OwnershipInfo)
	require.Equal(t, float64(1300), oi.ownership)
	ri := oi.ringItem.(*RingInstance)
	require.Equal(t, "instance-1", ri.instance)

	oi = heap.Pop(queue).(*OwnershipInfo)
	require.Equal(t, float64(950), oi.ownership)
	ri = oi.ringItem.(*RingInstance)
	require.Equal(t, "instance-0", ri.instance)

	oi = heap.Pop(queue).(*OwnershipInfo)
	require.Equal(t, float64(300), oi.ownership)
	ri = oi.ringItem.(*RingInstance)
	require.Equal(t, "instance-2", ri.instance)

	require.Equal(t, 0, queue.Len())
}

func TestSpreadMinimizingRing_AddFirstInstancePerZone(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	tokensPerInstance := 512
	ringDesc := &ring.Desc{}
	r := NewSpreadMinimizingRing(ringDesc, zones, tokensPerInstance, math.MaxUint32)
	require.NotNil(t, r)
	for _, zone := range zones {
		err := r.AddInstance(0, zone)
		require.NoError(t, err)
	}
	instancesByZone := r.getInstancesByZone()
	tokensByZone := r.getTokensByZone()
	zoneLen := len(zones)
	for z, zone := range zones {
		require.Len(t, instancesByZone[zone], 1)
		require.Len(t, tokensByZone[zone], tokensPerInstance)
		for i, token := range tokensByZone[zone] {
			require.Equal(t, uint32((i<<23/zoneLen)*zoneLen+z), token)
			require.Equal(t, z, int(token)%zoneLen)
		}
	}
}

func TestSpreadMinimizingRing_AddInstance(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 26
	tokensPerInstance := 512
	ringDesc := &ring.Desc{}
	r := NewSpreadMinimizingRing(ringDesc, zones, tokensPerInstance, math.MaxUint32)
	require.NotNil(t, r)
	for i := 0; i < instancesPerZone; i++ {
		for _, zone := range zones {
			err := r.AddInstance(i, zone)
			require.NoError(t, err)
		}
	}
	ownershipByInstanceByZone, ownershipByTokenByZone := r.getRegisteredOwnershipByZone()
	require.Len(t, ownershipByInstanceByZone[zones[0]], 26)
	require.Len(t, ownershipByTokenByZone[zones[0]], 26*tokensPerInstance)
}

func TestSpreadMinimizingRing_AddInstancePerScenario(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	tokensPerInstance := 512
	ringDesc := &ring.Desc{}
	r := NewSpreadMinimizingRing(ringDesc, zones, tokensPerInstance, math.MaxUint32)
	for i := 0; i <= 128; i++ {
		for _, zone := range zones {
			err := r.AddInstance(i, zone)
			require.NoError(t, err)
		}
		fmt.Printf("Ring with %d instances per zone, %d zones and %d tokens per instance\n", i+1, len(zones), tokensPerInstance)
		statistics := r.getStatistics()
		require.NotNil(t, statistics)
		statistics.Print()
		fmt.Printf("%s\n", strings.Repeat("-", 50))
	}
}
