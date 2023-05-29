package eventokendistributor

import (
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFixed(t *testing.T) {
	t.Run("generates a linearly incrementing amount of tokens", func(t *testing.T) {
		f := Fixed{}

		for i := 0; i < 10; i++ {
			tokens, err := f.CalculateTokens(i, 0, nil)
			require.NoError(t, err)
			require.Equal(t, i+1, len(tokens))
		}
	})
	t.Run("equally distributed", func(t *testing.T) {
		const instances = 7
		type tokenOwner struct {
			token    uint32
			instance int
		}
		f := Fixed{}
		var ring []tokenOwner
		for i := 0; i < instances; i++ {
			tokens, err := f.CalculateTokens(i, 0, nil)
			require.NoError(t, err)
			for _, t := range tokens {
				ring = append(ring, tokenOwner{t, i})
			}
		}
		sort.Slice(ring, func(i, j int) bool { return ring[i].token < ring[j].token })
		ownership := map[int]uint32{}
		tokensOwned := map[int]int{}
		for i, t := range ring {
			nextToken := uint32(math.MaxUint32)
			if i < len(ring)-1 {
				nextToken = ring[i+1].token
			}
			fmt.Println(t.token, nextToken)
			ownership[t.instance] += nextToken - t.token
			tokensOwned[t.instance]++
		}
		expectedOwnership := uint32(math.MaxUint32) / uint32(instances)
		t.Logf("Expected ownership: %d", expectedOwnership)
		for instance := 0; instance < instances; instance++ {
			owns := ownership[instance]
			t.Logf("Instance %d: %d in %d tokens", instance, owns, tokensOwned[instance])
			assert.InEpsilon(t, expectedOwnership, owns, 0.01, "instance %d", instance)
		}
	})
	t.Run("collisions", func(t *testing.T) {
		f := Fixed{}
		assigned := make(map[uint32]int)

		for i := 0; i < 10; i++ {
			tokens, err := f.CalculateTokens(i, 0, nil)
			require.NoError(t, err)
			for _, token := range tokens {
				if instance, ok := assigned[token]; ok {
					t.Fatalf("instance %d: token %d already assigned to instance %d", i, token, instance)
				}
				assigned[token] = i
			}
		}
	})
}
