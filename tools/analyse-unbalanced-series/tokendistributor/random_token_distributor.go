package tokendistributor

import "C"

type RandomTokenDistributor struct {
	*TokenDistributor
}

func NewRandomTokenDistributor(tokensPerInstance, zonesCount int, maxTokenValue Token, replicationStrategy ReplicationStrategy, seedGenerator SeedGenerator) *RandomTokenDistributor {
	return &RandomTokenDistributor{
		NewTokenDistributor(tokensPerInstance, zonesCount, maxTokenValue, replicationStrategy, seedGenerator),
	}
}
