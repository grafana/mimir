package tokendistributor

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func createRandomTokenDistributor(maxToken Token) *RandomTokenDistributor {
	tokenDistributor := createTokenDistributor(maxToken)
	return &RandomTokenDistributor{
		tokenDistributor,
	}
}

func TestRandomTokenDistributor_CalculateCandidateToken(t *testing.T) {
	randomTokenDistributor := createRandomTokenDistributor(maxToken)
	tokenInfoCircularList, _ := createTokenInfoCircularList(randomTokenDistributor.TokenDistributor, newInstance, newInstanceZone)
	fmt.Println(tokenInfoCircularList)
	candidateToken, err := randomTokenDistributor.calculateCandidateToken()
	require.Nil(t, err)
	require.False(t, slices.Contains(randomTokenDistributor.sortedTokens, candidateToken))
	hostIndexFloor := searchTokenFloor(randomTokenDistributor.sortedTokens, candidateToken)
	hostIndex := searchToken(randomTokenDistributor.sortedTokens, candidateToken)
	fmt.Println(candidateToken, hostIndexFloor, randomTokenDistributor.sortedTokens[hostIndexFloor], hostIndex, randomTokenDistributor.sortedTokens[hostIndex])
	hostIndexFloor = searchTokenFloor(randomTokenDistributor.sortedTokens, Token(910))
	hostIndex = searchToken(randomTokenDistributor.sortedTokens, Token(910))
	fmt.Println(910, hostIndexFloor, randomTokenDistributor.sortedTokens[hostIndexFloor], hostIndex, randomTokenDistributor.sortedTokens[hostIndex])
	hostIndexFloor = searchTokenFloor(randomTokenDistributor.sortedTokens, Token(20))
	hostIndex = searchToken(randomTokenDistributor.sortedTokens, Token(20))
	fmt.Println(20, hostIndexFloor, randomTokenDistributor.sortedTokens[hostIndexFloor], hostIndex, randomTokenDistributor.sortedTokens[hostIndex])
}

func TestRandomTokenDistributor_AddInstance(t *testing.T) {
	randomTokenDistributor := createRandomTokenDistributor(maxToken)
	_, _, ownershipInfo, err := randomTokenDistributor.AddInstance(newInstance, newInstanceZone)
	require.Nil(t, err)
	require.NotNil(t, ownershipInfo)
	_, ok := ownershipInfo.InstanceOwnershipMap[newInstance]
	require.True(t, ok)
	tokens, ok := randomTokenDistributor.tokensByInstance[newInstance]
	require.True(t, ok)
	require.Len(t, tokens, randomTokenDistributor.tokensPerInstance)
	newToken := tokens[0]
	require.True(t, slices.Contains(randomTokenDistributor.sortedTokens, newToken))
	require.Equal(t, newInstanceZone, randomTokenDistributor.zoneByInstance[newInstance])
}

func TestRandomTokenDistributor_AddFirstInstanceOfAZone(t *testing.T) {
	zones := []Zone{"zone-a", "zone-b", "zone-c"}
	replicationStrategy := NewZoneAwareReplicationStrategy(replicationFactor, make(map[Instance]Zone, initialInstanceCount), nil, nil)
	tokenDistributor := NewRandomTokenDistributor(tokensPerInstance, zonesCount, maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))
	instances := []Instance{"A", "B", "C"}

	for i := range instances {
		require.Len(t, tokenDistributor.sortedTokens, i*tokensPerInstance)
		_, ok := tokenDistributor.tokensByInstance[instances[i]]
		require.NotNil(t, ok)
		tokenDistributor.AddInstance(instances[i], zones[i])
		require.Len(t, tokenDistributor.sortedTokens, (i+1)*tokensPerInstance)
		_, ok = tokenDistributor.tokensByInstance[instances[i]]
		require.True(t, ok)
		slices.IsSorted(tokenDistributor.sortedTokens)
	}
}

func TestRandomTokenDistributor_AddSecondInstanceOfAZone(t *testing.T) {
	zones := []Zone{"zone-a", "zone-b", "zone-c"}
	replicationStrategy := NewZoneAwareReplicationStrategy(replicationFactor, make(map[Instance]Zone, initialInstanceCount), nil, nil)
	tokenDistributor := NewRandomTokenDistributor(tokensPerInstance, zonesCount, maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))
	tokenDistributor.maxTokenValue = maxToken
	instances := []Instance{"A-1", "B-1", "C-1"}

	for i := range instances {
		tokenDistributor.AddInstance(instances[i], zones[i])
	}
	instances = []Instance{"A-2", "B-2", "C-2", "A-3", "B-3", "C-3"}
	for i := range instances {
		tokenDistributor.AddInstance(instances[i], zones[i%len(zones)])
	}

	require.Len(t, tokenDistributor.sortedTokens, 9*tokensPerInstance)
}

func TestRandomTokenDistributor_GenerationZoneAware(t *testing.T) {
	iterations := 1
	zones := []Zone{"zone-a", "zone-b", "zone-c"}
	replicationFactor := len(zones)
	maxToken := Token(math.MaxUint32)
	numberOfInstancesPerZone := 22
	tokensPerInstance := 64
	for it := 0; it < iterations; it++ {
		fmt.Printf("Iteration %d...\n", it+1)
		replicationStrategy := NewZoneAwareReplicationStrategy(replicationFactor, make(map[Instance]Zone, initialInstanceCount), nil, nil)
		tokenDistributor := NewRandomTokenDistributor(tokensPerInstance, len(zones), maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))

		for i := 0; i < numberOfInstancesPerZone; i++ {
			for j := 0; j < len(zones); j++ {
				instance := Instance(fmt.Sprintf("%s-%d", string(rune('A'+j)), i))
				_, _, ownershipInfo, err := tokenDistributor.AddInstance(instance, zones[j])
				require.Nil(t, err)
				require.NotNil(t, ownershipInfo)
				fmt.Printf("Instance %s added\n", instance)
			}
		}
		require.Len(t, tokenDistributor.sortedTokens, len(zones)*tokensPerInstance*numberOfInstancesPerZone)
	}
}

func TestRandomTokenDistributor_GenerationNoReplication(t *testing.T) {
	iterations := 10
	replicationFactor := 1
	maxToken := Token(math.MaxUint32)
	zones := []Zone{SingleZone}
	numberOfInstancesPerZone := 20
	tokensPerInstance := 64

	for it := 0; it < iterations; it++ {
		fmt.Printf("Iteration %d...\n", it+1)
		replicationStrategy := NewSimpleReplicationStrategy(replicationFactor, nil)
		tokenDistributor := NewRandomTokenDistributor(tokensPerInstance, len(zones), maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))

		for i := 0; i < numberOfInstancesPerZone; i++ {
			instance := Instance(fmt.Sprintf("instance-%d", i))
			_, _, ownershipInfo, _ := tokenDistributor.AddInstance(instance, SingleZone)
			require.NotNil(t, ownershipInfo)
			fmt.Printf("Instance %s added\n", instance)
		}
		require.Len(t, tokenDistributor.sortedTokens, tokensPerInstance*numberOfInstancesPerZone)
	}
}

func TestRandomTokenDistributor_GenerationReplicationWithoutZones(t *testing.T) {
	iterations := 10
	replicationFactor := 3
	maxToken := Token(maxTokenValue)
	zones := []Zone{SingleZone}
	numberOfInstancesPerZone := 66
	tokensPerInstance := 64

	for it := 0; it < iterations; it++ {
		fmt.Printf("Iteration %d...\n", it+1)
		replicationStrategy := NewSimpleReplicationStrategy(replicationFactor, nil)
		tokenDistributor := NewRandomTokenDistributor(tokensPerInstance, len(zones), maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))

		for i := 0; i < numberOfInstancesPerZone; i++ {
			instance := Instance(fmt.Sprintf("instance-%d", i))
			_, _, ownershipInfo, _ := tokenDistributor.AddInstance(instance, SingleZone)
			require.NotNil(t, ownershipInfo)
			fmt.Printf("Instance %s added\n", instance)
		}
		require.Len(t, tokenDistributor.sortedTokens, tokensPerInstance*numberOfInstancesPerZone)
	}
}
