package eventokendistributor

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestRing_GetInstancesByZone(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 2
	tokensPerInstance := 4
	tokensByInstanceID := createTokensByInstanceID(instancesPerZone, tokensPerInstance, zones)
	ringDesc := createRigDesc(instancesPerZone, zones, tokensByInstanceID)
	td := NewPower2TokenDistributor(zones, tokensPerInstance, math.MaxUint32)
	r := NewRing(ringDesc, zones, tokensPerInstance, math.MaxUint32, td)
	instancesByZone := r.getInstancesByZone()
	for i := 0; i < instancesPerZone; i++ {
		for _, zone := range zones {
			instances, ok := instancesByZone[zone]
			require.True(t, ok)
			instanceID := fmt.Sprintf("%d-%s", i, zone)
			instance, ok := instances[instanceID]
			require.True(t, ok)
			tokens := ring.Tokens(instance.GetTokens())
			require.True(t, tokensByInstanceID[instanceID].Equals(tokens))
		}
	}
}

func TestRing_GetTokensByZone(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 2
	tokensPerInstance := 4
	tokensByInstanceID := createTokensByInstanceID(instancesPerZone, tokensPerInstance, zones)
	ringDesc := createRigDesc(instancesPerZone, zones, tokensByInstanceID)
	r := NewRing(ringDesc, zones, tokensPerInstance, math.MaxUint32, nil)
	tokensByZone := r.getTokensByZone()
	for _, zone := range zones {
		tokens, ok := tokensByZone[zone]
		require.True(t, ok)
		require.True(t, slices.IsSorted(tokens))
		for i := 0; i < instancesPerZone; i++ {
			instanceID := fmt.Sprintf("%d-%s", i, zone)
			for _, token := range tokensByZone[instanceID] {
				slices.Contains(tokens, token)
			}
		}
	}
}

func TestRing_GetRegisteredOwnershipByInstanceByZone(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 2
	tokensPerInstance := 4
	tokensByInstanceID := createTokensByInstanceID(instancesPerZone, tokensPerInstance, zones)
	ringDesc := createRigDesc(instancesPerZone, zones, tokensByInstanceID)
	td := NewPower2TokenDistributor(zones, tokensPerInstance, 1000)
	r := NewRing(ringDesc, zones, tokensPerInstance, 1000, td)
	ownershipByInstanceByZone := r.getRegisteredOwnershipByInstanceByZone()
	require.Len(t, ownershipByInstanceByZone, len(zones))
	for _, ownershipByZone := range ownershipByInstanceByZone {
		require.Len(t, ownershipByZone, instancesPerZone)
	}
	tokensByZone := r.getTokensByZone()
	for instanceID, instance := range r.ringDesc.GetIngesters() {
		tokens := instance.GetTokens()
		allTokens := tokensByZone[instance.Zone]
		ownership := 0.0
		for _, token := range tokens {
			index := search(allTokens, token)
			if index == -1 {
				panic(fmt.Errorf("token %d not found", token))
			}
			prev := index - 1
			if prev < 0 {
				prev = len(allTokens) - 1
			}
			ownership += float64(distance(allTokens[prev], token, td.maxTokenValue))
		}
		require.Equal(t, ownership, ownershipByInstanceByZone[instance.Zone][instanceID])
	}
}

func TestRing_GetRegisteredOwnershipByZone(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 2
	tokensPerInstance := 4
	tokensByInstanceID := createTokensByInstanceID(instancesPerZone, tokensPerInstance, zones)
	ringDesc := createRigDesc(instancesPerZone, zones, tokensByInstanceID)
	td := NewPower2TokenDistributor(zones, tokensPerInstance, 1000)
	r := NewRing(ringDesc, zones, tokensPerInstance, 1000, td)
	ownershipByInstanceByZone, ownerwshipByTokenByZone := r.getRegisteredOwnershipByZone()
	require.Len(t, ownershipByInstanceByZone, len(zones))
	for _, ownershipByZone := range ownershipByInstanceByZone {
		require.Len(t, ownershipByZone, instancesPerZone)
	}
	tokensByZone := r.getTokensByZone()
	for instanceID, instance := range r.ringDesc.GetIngesters() {
		tokens := instance.GetTokens()
		allTokens := tokensByZone[instance.Zone]
		ownership := 0.0
		for _, token := range tokens {
			index := search(allTokens, token)
			if index == -1 {
				panic(fmt.Errorf("token %d not found", token))
			}
			prev := index - 1
			if prev < 0 {
				prev = len(allTokens) - 1
			}
			tokenOwnership := float64(distance(allTokens[prev], token, td.maxTokenValue))
			ownership += tokenOwnership
			require.Equal(t, tokenOwnership, ownerwshipByTokenByZone[instance.Zone][token])
		}
		require.Equal(t, ownership, ownershipByInstanceByZone[instance.Zone][instanceID])
	}
}

func TestRing_GetStatistics(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 2
	tokensPerInstance := 4
	tokensByInstanceID := createTokensByInstanceID(instancesPerZone, tokensPerInstance, zones)
	ringDesc := createRigDesc(instancesPerZone, zones, tokensByInstanceID)
	td := NewPower2TokenDistributor(zones, tokensPerInstance, 1000)
	r := NewRing(ringDesc, zones, tokensPerInstance, math.MaxUint32, td)
	ownershipByInstanceByZone := r.getRegisteredOwnershipByInstanceByZone()
	require.Len(t, ownershipByInstanceByZone, len(zones))
	for _, ownershipByZone := range ownershipByInstanceByZone {
		require.Len(t, ownershipByZone, instancesPerZone)
	}
	statistics := r.getStatistics()
	require.NotNil(t, statistics)
	for z, zone := range statistics.zones {
		min := statistics.minByZone[z]
		max := statistics.maxByZone[z]
		ownershipByInstance := ownershipByInstanceByZone[zone]
		for _, ownership := range ownershipByInstance {
			require.LessOrEqual(t, min, ownership)
			require.GreaterOrEqual(t, max, ownership)
		}
		require.Equal(t, (max-min)*100/max, statistics.spreadByZone[z])
	}
}

func TestRing_AddInstance(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 120
	tokensPerInstance := 512
	ringDesc := &ring.Desc{}
	td := NewPower2TokenDistributor(zones, tokensPerInstance, math.MaxUint32)
	r := NewRing(ringDesc, zones, tokensPerInstance, math.MaxUint32, td)
	for i := 0; i < instancesPerZone; i++ {
		for _, zone := range zones {
			err := r.AddInstance(i, zone)
			require.NoError(t, err)
		}
	}
	instancesByZone := r.getInstancesByZone()
	for _, zone := range zones {
		instances, ok := instancesByZone[zone]
		require.True(t, ok)
		require.Len(t, instances, instancesPerZone)
		for _, instance := range instances {
			tokens := instance.GetTokens()
			require.Len(t, tokens, tokensPerInstance)
			require.True(t, slices.IsSorted(tokens))
		}
	}
}

func TestRing_AddInstancePerScenario(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZoneScenarios := []int{0, 1, 2, 3, 4, 5, 15, 16, 17, 63, 64, 65, 127, 128, 129}
	tokensPerInstance := 512
	for _, instancesPerZone := range instancesPerZoneScenarios {
		td := NewPower2TokenDistributor(zones, tokensPerInstance, math.MaxUint32)
		ringDesc := &ring.Desc{}
		r := NewRing(ringDesc, zones, tokensPerInstance, math.MaxUint32, td)
		for i := 0; i < instancesPerZone; i++ {
			for _, zone := range zones {
				err := r.AddInstance(i, zone)
				require.NoError(t, err)
			}
		}
		fmt.Printf("Ring with %d instances per zone, %d zones and %d tokens per instance\n", instancesPerZone, len(zones), tokensPerInstance)
		statistics := r.getStatistics()
		require.NotNil(t, statistics)
		statistics.Print()
		fmt.Printf("%s\n", strings.Repeat("-", 50))
	}
}

func createTokensByInstanceID(instancesPerZone, tokensPerInstance int, zones []string) map[string]ring.Tokens {
	step := uint32(5)
	start := uint32(0)
	offset := step * uint32(instancesPerZone*len(zones))
	tokensByInstance := map[string]ring.Tokens{}
	for i := 0; i < instancesPerZone; i++ {
		for _, zone := range zones {
			instanceID := fmt.Sprintf("%d-%s", i, zone)
			tokens := make(ring.Tokens, 0, tokensPerInstance)
			curr := start + step
			for t := 0; t < tokensPerInstance; t++ {
				tokens = append(tokens, curr)
				curr += offset
			}
			start++
			tokensByInstance[instanceID] = tokens
		}
	}
	return tokensByInstance
}

func createRigDesc(instancesPerZone int, zones []string, tokensByInstanceID map[string]ring.Tokens) *ring.Desc {
	instances := map[string]ring.InstanceDesc{}
	for i := 0; i < instancesPerZone; i++ {
		for _, zone := range zones {
			instanceID := fmt.Sprintf("%d-%s", i, zone)
			instances[instanceID] = ring.InstanceDesc{
				Addr:                instanceID,
				Timestamp:           0,
				State:               0,
				Tokens:              tokensByInstanceID[instanceID],
				Zone:                zone,
				RegisteredTimestamp: 0,
			}
		}
	}
	return &ring.Desc{
		Ingesters: instances,
	}
}

func search(tokens ring.Tokens, token uint32) int {
	i := sort.Search(len(tokens), func(x int) bool {
		return tokens[x] >= token
	})
	if i >= len(tokens) {
		i = -1
	}
	return i
}
