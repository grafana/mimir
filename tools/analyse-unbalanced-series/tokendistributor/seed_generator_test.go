package tokendistributor

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestRandomSeedGenerator_GenerateMultiZone(t *testing.T) {
	zones := []Zone{"a", "b", "c"}
	replicationFactor := 3
	tokensPerInstance := 4
	maxToken := Token(1000)
	seedGenerator := NewRandomSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken)
	seedByZone := make(map[Zone][]Token, len(zones))
	for i := range zones {
		require.True(t, seedGenerator.hasNextSeed(zones[i]))
		seeds, ok := seedGenerator.getNextSeed(zones[i])
		require.NotNil(t, seeds)
		require.True(t, ok)
		require.Len(t, seeds, tokensPerInstance)
		require.True(t, slices.IsSorted(seeds))
		require.False(t, seedGenerator.hasNextSeed(zones[i]))
		seedByZone[zones[i]] = seeds
	}

	for i := 0; i < tokensPerInstance; i++ {
		for j := 0; j < len(zones)-1; j++ {
			require.True(t, seedByZone[zones[j]][i] < seedByZone[zones[j+1]][i])
		}
	}
}

func TestRandomSeedGenerator_GenerateSingleZoneNoReplication(t *testing.T) {
	zones := []Zone{SingleZone}
	replicationFactor := 1
	tokensPerInstance := 4
	maxToken := Token(1000)
	seedGenerator := NewRandomSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken)
	seedByZone := make(map[Zone][]Token, len(zones))
	for i := range zones {
		require.True(t, seedGenerator.hasNextSeed(zones[i]))
		seeds, ok := seedGenerator.getNextSeed(zones[i])
		require.NotNil(t, seeds)
		require.True(t, ok)
		require.Len(t, seeds, tokensPerInstance)
		require.True(t, slices.IsSorted(seeds))
		require.False(t, seedGenerator.hasNextSeed(zones[i]))
		seedByZone[zones[i]] = seeds
	}

	for i := 0; i < tokensPerInstance; i++ {
		for j := 0; j < len(zones)-1; j++ {
			require.True(t, seedByZone[zones[j]][i] < seedByZone[zones[j+1]][i])
		}
	}
}

func TestRandomSeedGenerator_GenerateSingleZoneWithReplication(t *testing.T) {
	zones := []Zone{SingleZone}
	replicationFactor := 3
	tokensPerInstance := 4
	maxToken := Token(1000)
	seedGenerator := NewRandomSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken)
	allSeeds := make([][]Token, 0, replicationFactor)
	for j := 0; j < replicationFactor; j++ {
		require.True(t, seedGenerator.hasNextSeed(SingleZone))
		seeds, ok := seedGenerator.getNextSeed(SingleZone)
		require.NotNil(t, seeds)
		require.True(t, ok)
		require.Len(t, seeds, tokensPerInstance)
		require.True(t, slices.IsSorted(seeds))
		allSeeds = append(allSeeds, make([]Token, 0, tokensPerInstance))
		allSeeds[j] = append(allSeeds[j], seeds...)
	}

	require.False(t, seedGenerator.hasNextSeed(SingleZone))

	for i := 0; i < tokensPerInstance; i++ {
		for j := 0; j < replicationFactor-1; j++ {
			require.True(t, allSeeds[j][i] < allSeeds[j+1][i])
		}
	}
}

func TestPerfectlySpacedSeedGenerator_GenerateMultiZone(t *testing.T) {
	zones := []Zone{"a", "b", "c"}
	replicationFactor := 3
	tokensPerInstance := 4
	maxToken := Token(1000)
	seedGenerator := NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken)
	seedByZone := make(map[Zone][]Token, len(zones))
	for i := range zones {
		require.True(t, seedGenerator.hasNextSeed(zones[i]))
		seeds, ok := seedGenerator.getNextSeed(zones[i])
		require.NotNil(t, seeds)
		require.True(t, ok)
		require.Len(t, seeds, tokensPerInstance)
		require.True(t, slices.IsSorted(seeds))
		require.False(t, seedGenerator.hasNextSeed(zones[i]))
		seedByZone[zones[i]] = seeds
	}

	for i := 0; i < tokensPerInstance; i++ {
		for j := 0; j < len(zones)-1; j++ {
			require.True(t, seedByZone[zones[j]][i] < seedByZone[zones[j+1]][i])
		}
	}
}

func TestPerfectlySpacedSeedGenerator_GenerateSingleZoneNoReplication(t *testing.T) {
	zones := []Zone{SingleZone}
	replicationFactor := 1
	tokensPerInstance := 4
	maxToken := Token(1000)
	seedGenerator := NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken)
	seedByZone := make(map[Zone][]Token, len(zones))
	for i := range zones {
		require.True(t, seedGenerator.hasNextSeed(zones[i]))
		seeds, ok := seedGenerator.getNextSeed(zones[i])
		require.NotNil(t, seeds)
		require.True(t, ok)
		require.Len(t, seeds, tokensPerInstance)
		require.True(t, slices.IsSorted(seeds))
		require.False(t, seedGenerator.hasNextSeed(zones[i]))
		seedByZone[zones[i]] = seeds
	}

	for i := 0; i < tokensPerInstance; i++ {
		for j := 0; j < len(zones)-1; j++ {
			require.True(t, seedByZone[zones[j]][i] < seedByZone[zones[j+1]][i])
		}
	}
}

func TestPerfectlySpacedSeedGenerator_GenerateSingleZoneWithReplication(t *testing.T) {
	zones := []Zone{SingleZone}
	replicationFactor := 3
	tokensPerInstance := 4
	maxToken := Token(1000)
	seedGenerator := NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken)
	allSeeds := make([][]Token, 0, replicationFactor)
	for j := 0; j < replicationFactor; j++ {
		require.True(t, seedGenerator.hasNextSeed(SingleZone))
		seeds, ok := seedGenerator.getNextSeed(SingleZone)
		require.NotNil(t, seeds)
		require.True(t, ok)
		require.Len(t, seeds, tokensPerInstance)
		require.True(t, slices.IsSorted(seeds))
		allSeeds = append(allSeeds, make([]Token, 0, tokensPerInstance))
		allSeeds[j] = append(allSeeds[j], seeds...)
	}

	require.False(t, seedGenerator.hasNextSeed(SingleZone))

	for i := 0; i < tokensPerInstance; i++ {
		for j := 0; j < replicationFactor-1; j++ {
			require.True(t, allSeeds[j][i] < allSeeds[j+1][i])
		}
	}
}

func Test(t *testing.T) {
	zonesCount := 3
	tokensPerInstanceCount := 512
	tokensPerZone := make([][]uint32, 0, zonesCount)
	for z := 0; z < zonesCount; z++ {
		tokensPerZone = append(tokensPerZone, make([]uint32, 0, tokensPerInstanceCount))
		for t := 0; t < tokensPerInstanceCount; t++ {
			token := uint32(math.Pow(2, 32)*(1.0-(3.0*float64(t)+float64(z)))/float64(zonesCount*tokensPerInstanceCount)) - 1
			tokensPerZone[z] = append(tokensPerZone[z], token)
		}
		slices.Sort(tokensPerZone[z])
	}

	fmt.Printf("Perfect distance: %d\n", int(float64(math.MaxUint32)/float64(tokensPerInstanceCount)))
	for z := 0; z < zonesCount; z++ {
		fmt.Printf("Zone %c\n", rune('A'+z))
		for t := 1; t < tokensPerInstanceCount; t++ {
			fmt.Printf("\ttoken: %12d, next token: %12d, distance: %12d\n", tokensPerZone[z][t-1], tokensPerZone[z][t], tokensPerZone[z][t]-tokensPerZone[z][t-1])
		}
	}

	fmt.Println(tokensPerZone)
}
