package tokendistributor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func createTokenDistributor() *TokenDistributor {
	sortedRingTokens, ringInstanceByToken, zoneByInstance := createRingTokensInstancesZones()
	replicationStrategy := newZoneAwareReplicationStrategy(3, zoneByInstance, nil, nil)
	tokenDistributor := newTokenDistributorFromInitializedInstances(sortedRingTokens, ringInstanceByToken, 4, replicationStrategy)
	tokenDistributor.maxTokenValue = 1000
	return tokenDistributor
}

func TestTokenDistributor_CreateTokenInfoCircularList(t *testing.T) {
	tokenDistributor := createTokenDistributor()
	infoInstanceByInstance, zoneInfoByZone := tokenDistributor.createInstanceAndZoneInfos()
	tokenInfoCircularList := tokenDistributor.createTokenInfoCircularList(infoInstanceByInstance, zoneInfoByZone["zone-b"])
	for _, token := range tokenDistributor.sortedTokens {
		head := tokenInfoCircularList.head
		fmt.Println(head.getData())
		require.Equal(t, head.getData().getToken(), token)
		tokenInfoCircularList.remove(head)
	}
}

func TestTokenDistributor_CreateCandidateTokenInfoCircularList(t *testing.T) {
	tokenDistributor := createTokenDistributor()
	infoInstanceByInstance, zoneInfoByZone := tokenDistributor.createInstanceAndZoneInfos()
	tokenInfoCircularList := tokenDistributor.createTokenInfoCircularList(infoInstanceByInstance, zoneInfoByZone["zone-b"])
	newInstanceInfo := newInstanceInfo("instance-4", zoneInfoByZone["zone-b"], 4)
	optimalTokenOwnership := tokenDistributor.getOptimalTokenOwnership(4)
	candidateTokenCircularList := tokenDistributor.createCandidateTokenInfoCircularList(tokenInfoCircularList, newInstanceInfo, optimalTokenOwnership)

	curr1 := tokenInfoCircularList.head
	curr2 := candidateTokenCircularList.head
	for _ = range tokenDistributor.sortedTokens {
		candidateToken := tokenDistributor.calculateCandidateToken(curr1.getData())
		require.Equal(t, curr2.getData().getToken(), candidateToken)
		require.Equal(t, curr2.getData().getPrevious(), curr1.getData())
		fmt.Println(curr2.getData())
		curr1 = curr1.next
		curr2 = curr2.next
	}
}

func TestTokenDistributor_EvaluateImprovement(t *testing.T) {
	tokenDistributor := createTokenDistributor()
	infoInstanceByInstance, zoneInfoByZone := tokenDistributor.createInstanceAndZoneInfos()
	tokenInfoCircularList := tokenDistributor.createTokenInfoCircularList(infoInstanceByInstance, zoneInfoByZone["zone-b"])
	newTokensCount := 4
	optimalTokenOwnership := tokenDistributor.getOptimalTokenOwnership(newTokensCount)
	newInstanceInfo := newInstanceInfo("instance-4", zoneInfoByZone["zone-b"], newTokensCount)
	newInstanceInfo.ownership = float64(newTokensCount) * optimalTokenOwnership
	candidateTokenCircularList := tokenDistributor.createCandidateTokenInfoCircularList(tokenInfoCircularList, newInstanceInfo, optimalTokenOwnership)
	head := candidateTokenCircularList.head
	curr := head
	for {
		candidate := curr.getData()
		improvement := tokenDistributor.evaluateImprovement(candidate, optimalTokenOwnership, 1/float64(newTokensCount))
		fmt.Printf("Improvement of insertion of candidate %s would be %.2f\n", candidate, improvement)
		curr = curr.next
		if curr == head {
			break
		}
	}
}
