package tokendistributor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpreadReducingTokenDistributor_GenerationReplicationWithoutZones(t *testing.T) {
	iterations := 10
	replicationFactor := 1
	maxToken := Token(maxTokenValue)
	zones := []Zone{SingleZone}
	numberOfInstancesPerZone := 66
	tokensPerInstance := 64

	for it := 0; it < iterations; it++ {
		fmt.Printf("Iteration %d...\n", it+1)
		replicationStrategy := NewSimpleReplicationStrategy(replicationFactor, nil)
		tokenDistributor := NewSpreadReducingTokenDistributor(tokensPerInstance, len(zones), maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))
		var ownershipInfo *OwnershipInfo
		for i := 0; i < numberOfInstancesPerZone; i++ {
			instance := Instance(fmt.Sprintf("instance-%d", i))
			_, _, ownershipInfo, _ = tokenDistributor.AddInstance(instance, SingleZone)
			require.NotNil(t, ownershipInfo)
			ownershipInfo.Print()
			//fmt.Printf("Instance %s added\n", instance)
		}
		require.Len(t, tokenDistributor.sortedTokens, tokensPerInstance*numberOfInstancesPerZone)
	}
}
