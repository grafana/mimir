package tokendistributor

import "C"
import (
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/exp/slices"
)

var (
	errorAllTokensUsed = fmt.Errorf("impossible to find a new token, all tokens have been used")
	errorHostToken     = fmt.Errorf("impossible to find host token")
)

type RandomTokenDistributor struct {
	*TokenDistributor
}

func NewRandomTokenDistributor(tokensPerInstance, zonesCount int, maxTokenValue Token, replicationStrategy ReplicationStrategy, seedGenerator SeedGenerator) *RandomTokenDistributor {
	return &RandomTokenDistributor{
		NewTokenDistributor(tokensPerInstance, zonesCount, maxTokenValue, replicationStrategy, seedGenerator),
	}
}

func (t *RandomTokenDistributor) calculateCandidateToken() (Token, error) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	if len(t.instanceByToken) == int(t.maxTokenValue) {
		return 0, errorAllTokensUsed
	}

	candidate := t.getNextRandomCandidate(r)
	for slices.Contains(t.sortedTokens, candidate) {
		candidate = t.getNextRandomCandidate(r)
	}
	return candidate, nil
}

func (t *RandomTokenDistributor) createCandidateTokenInfo(tokenInfoCircularList *CircularList[*tokenInfo], newInstanceInfo *instanceInfo) (*candidateTokenInfo, error) {
	candidateToken, err := t.calculateCandidateToken()
	if err != nil {
		return nil, err
	}
	hostIndex := searchToken(t.sortedTokens, candidateToken)
	if hostIndex == 0 {
		hostIndex = len(t.sortedTokens) - 1
	}
	prev := tokenInfoCircularList.head.prev
	curr := tokenInfoCircularList.head
	for {
		if curr.getData().getToken() > candidateToken {
			break
		}
		prev = curr
		curr = curr.next
		if curr == tokenInfoCircularList.head {
			return nil, errorHostToken
		}
	}
	return newCandidateTokenInfo(newInstanceInfo, candidateToken, prev.getData()), nil
}

func (t *RandomTokenDistributor) getNextRandomCandidate(rand *rand.Rand) Token {
	if t.maxTokenValue == maxTokenValue {
		return Token(rand.Uint32())
	}
	return Token(randU32(0, uint32(t.maxTokenValue)))
}

func randU32(min, max uint32) uint32 {
	var a = rand.Uint32()
	a %= max - min
	a += min
	return a
}
