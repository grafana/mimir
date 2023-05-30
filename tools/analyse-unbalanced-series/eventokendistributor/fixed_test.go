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

		for i := 0; i < 2; i++ {
			tokens, err := f.CalculateTokens(i, 0, nil)
			require.NoError(t, err)
			require.Equal(t, 1, len(tokens))
		}
		for i := 2; i < 100; i++ {
			t.Run(fmt.Sprintf("%d instances", i), func(t *testing.T) {
				tokens, err := f.CalculateTokens(i, 0, nil)
				require.NoError(t, err)
				require.Equal(t, i, len(tokens))
			})
		}
	})
	t.Run("equally distributed", func(t *testing.T) {
		for _, instances := range []int{2, 10, 20, 50, 66, 100} {
			t.Run(fmt.Sprintf("%d instances", instances), func(t *testing.T) {
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
		}
	})
	t.Run("collisions", func(t *testing.T) {
		type instanceInZone struct {
			instance, zone int
		}
		for offset := range [512]int{} {
			t.Run(fmt.Sprintf("offset %d", offset), func(t *testing.T) {
				f := Fixed{zoneOffset: offset}
				assigned := make(map[uint32]instanceInZone)

				for _, zone := range []int{0, 1, 2} {
					for i := 0; i < 256; i++ {
						tokens, err := f.CalculateTokens(i, zone, nil)
						require.NoError(t, err)
						for _, token := range tokens {
							if repeated, ok := assigned[token]; ok {
								t.Fatalf("instance %d in zone %d: token %d already assigned to instance %d in zone %d", i, zone, token, repeated.instance, repeated.zone)
							}
							assigned[token] = instanceInZone{i, zone}
						}
					}
				}
			})
		}

	})
}
