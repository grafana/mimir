package tokendistributor

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestRandomSeedGenerator_Generate(t *testing.T) {
	seedGenerator := RandomSeedGenerator{}
	zones := []Zone{"a", "b", "c"}
	seedByZone := seedGenerator.generateSeedByZone(zones, 4, 1000)
	for i := range zones {
		seed := seedByZone[zones[i]]
		require.NotNil(t, seed)
		require.Len(t, seed, tokensPerInstance)
		require.True(t, slices.IsSorted(seed))
	}

	for i := 0; i < tokensPerInstance; i++ {
		for j := 0; j < len(zones)-1; j++ {
			require.True(t, seedByZone[zones[j]][i] < seedByZone[zones[j+1]][i])
		}
	}
}

func TestPerfectlySpacedSeedGenerator_Generate(t *testing.T) {
	seedGenerator := PerfectlySpacedSeedGenerator{}
	zones := []Zone{"a", "b", "c"}
	tokensPerInstance := 4
	maxToken := Token(1000)
	seedByZone := seedGenerator.generateSeedByZone(zones, tokensPerInstance, maxToken)
	for i := range zones {
		seed := seedByZone[zones[i]]
		require.NotNil(t, seed)
		require.Len(t, seed, tokensPerInstance)
		require.True(t, slices.IsSorted(seed))
	}

	for i := 0; i < tokensPerInstance; i++ {
		for j := 0; j < len(zones)-1; j++ {
			require.True(t, seedByZone[zones[j]][i] < seedByZone[zones[j+1]][i])
		}
	}

	fmt.Println(seedByZone)

	tokensCount := tokensPerInstance * len(zones)
	offset := uint32(math.Ceil(float64(maxTokenValue) / float64(tokensCount)))
	fmt.Println(offset)
	diff := Token(offset * uint32(len(zones)))

	for i := 0; i < tokensPerInstance-1; i++ {
		for j := 0; j < len(zones); j++ {
			require.LessOrEqual(t, seedByZone[zones[j]][i+1]-seedByZone[zones[j]][i+1], diff)
		}
	}
}
