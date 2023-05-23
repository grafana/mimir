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

func (t *RandomTokenDistributor) AddInstance(instance Instance, zone Zone) (*CircularList[*tokenInfo], *CircularList[*candidateTokenInfo], *OwnershipInfo, error) {
	t.replicationStrategy.addInstance(instance, zone)
	if t.seedGenerator != nil && t.seedGenerator.hasNextSeed(zone) {
		t.addTokensFromSeed(instance, zone)
		return nil, nil, &OwnershipInfo{}, nil
	}

	optimalTokenOwnership := t.getOptimalTokenOwnership(true)
	for i := 0; i < t.tokensPerInstance; i++ {
		candidateToken, err := t.calculateCandidateToken()
		if err != nil {
			return nil, nil, nil, err
		}
		t.addNewInstanceAndToken(instance, zone, candidateToken)
	}

	instanceInfoByInstance, _ := t.createInstanceAndZoneInfos(&zone, &instance)
	newInstanceInfo := instanceInfoByInstance[instance]
	instanceInfoByInstance[instance] = newInstanceInfo
	tokenInfoCircularList := t.createTokenInfoCircularList(instanceInfoByInstance, newInstanceInfo)
	//fmt.Printf("\t\t\t%s", instanceInfoByInstance)
	ownershipInfo := t.createOwnershipInfo(tokenInfoCircularList, optimalTokenOwnership)

	//t.count(tokenInfoCircularList)
	//fmt.Println("-----------------------------------------------------------")
	return tokenInfoCircularList, nil, ownershipInfo, nil
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
