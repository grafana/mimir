package ring

import (
	"math/rand"
	"sort"
	"time"
)

type TokenGenerator interface {
	// GenerateTokens generates unique tokensCount tokens, none of which clash
	// with the given takenTokens. Generated tokens are sorted.
	GenerateTokens(tokensCount int, takenTokens []uint32) Tokens
}

type RandomTokenGenerator struct{}

func NewRandomTokenGenerator() *RandomTokenGenerator {
	return &RandomTokenGenerator{}
}

// GenerateTokens generates unique tokensCount random tokens, none of which clash
// with takenTokens. Generated tokens are sorted.
func (t *RandomTokenGenerator) GenerateTokens(tokensCount int, takenTokens []uint32) Tokens {
	if tokensCount <= 0 {
		return []uint32{}
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	used := make(map[uint32]bool, len(takenTokens))
	for _, v := range takenTokens {
		used[v] = true
	}

	tokens := make([]uint32, 0, tokensCount)
	for i := 0; i < tokensCount; {
		candidate := r.Uint32()
		if used[candidate] {
			continue
		}
		used[candidate] = true
		tokens = append(tokens, candidate)
		i++
	}

	// Ensure returned tokens are sorted.
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i] < tokens[j]
	})

	return tokens
}
