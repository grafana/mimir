package tokendistributor

import (
	"container/heap"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
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
	bestOwnershipDecrease := math.MaxFloat64
	bestCandidate := head
	for {
		candidate := curr.getData()
		improvement := tokenDistributor.evaluateImprovement(candidate, optimalTokenOwnership, 1/float64(newTokensCount))
		fmt.Printf("Improvement of insertion of candidate %s would be %.2f\n", candidate, improvement)

		if improvement < bestOwnershipDecrease {
			bestOwnershipDecrease = improvement
			bestCandidate = curr
		}

		curr = curr.next
		if curr == head {
			break
		}
	}

	require.Equal(t, Token(770), bestCandidate.getData().getToken())
}

func TestTokenDistributor_CreatePriorityQueue(t *testing.T) {
	tokenDistributor := createTokenDistributor()
	infoInstanceByInstance, zoneInfoByZone := tokenDistributor.createInstanceAndZoneInfos()
	tokenInfoCircularList := tokenDistributor.createTokenInfoCircularList(infoInstanceByInstance, zoneInfoByZone["zone-b"])
	newTokensCount := 4
	optimalTokenOwnership := tokenDistributor.getOptimalTokenOwnership(newTokensCount)
	newInstanceInfo := newInstanceInfo("instance-4", zoneInfoByZone["zone-b"], newTokensCount)
	newInstanceInfo.ownership = float64(newTokensCount) * optimalTokenOwnership
	candidateTokenCircularList := tokenDistributor.createCandidateTokenInfoCircularList(tokenInfoCircularList, newInstanceInfo, optimalTokenOwnership)

	sortedImprovements := make([]float64, 0, len(tokenDistributor.sortedTokens))
	head := candidateTokenCircularList.head
	curr := head
	for {
		candidate := curr.getData()
		improvement := tokenDistributor.evaluateImprovement(candidate, optimalTokenOwnership, 1/float64(newTokensCount))
		sortedImprovements = append(sortedImprovements, improvement)
		curr = curr.next
		if curr == head {
			break
		}
	}

	slices.Sort(sortedImprovements)

	pq := tokenDistributor.createPriorityQueue(candidateTokenCircularList, optimalTokenOwnership, newTokensCount)
	for _, improvement := range sortedImprovements {
		improvementFromPQ := heap.Pop(pq).(*CandidateTokenInfoImprovement).improvement
		require.Equal(t, improvement, improvementFromPQ)
	}
}

func TestTokenDistributor_VerifyTokenInfo(t *testing.T) {
	tokenDistributor := createTokenDistributor()
	infoInstanceByInstance, zoneInfoByZone := tokenDistributor.createInstanceAndZoneInfos()
	tokenInfoCircularList := tokenDistributor.createTokenInfoCircularList(infoInstanceByInstance, zoneInfoByZone["zone-b"])
	require.True(t, verifyReplicaStartAndReplicatedOwnership(t, tokenDistributor, tokenInfoCircularList))

	newTokensCount := 4
	optimalTokenOwnership := tokenDistributor.getOptimalTokenOwnership(newTokensCount)
	newInstanceInfo := newInstanceInfo("instance-4", zoneInfoByZone["zone-b"], newTokensCount)
	newInstanceInfo.ownership = float64(newTokensCount) * optimalTokenOwnership
	tokenDistributor.createCandidateTokenInfoCircularList(tokenInfoCircularList, newInstanceInfo, optimalTokenOwnership)
	require.True(t, verifyReplicaStartAndReplicatedOwnership(t, tokenDistributor, tokenInfoCircularList))
}

func verifyReplicaStartAndReplicatedOwnership(t *testing.T, tokenDistributor *TokenDistributor, tokenInfoCircularList *CircularList[*tokenInfo]) bool {
	replicaStartMap := make(map[Token]Token, len(tokenDistributor.sortedTokens))
	ownershipMap := make(map[Instance]float64, len(tokenDistributor.tokensByInstance))
	head := tokenInfoCircularList.head
	curr := head
	for {
		replicaStartMap[curr.getData().getToken()] = curr.getData().getReplicaStart().getToken()
		ownership, ok := ownershipMap[curr.getData().getOwningInstance().instanceId]
		if !ok {
			ownership = 0
		}
		ownership += curr.getData().getReplicatedOwnership()
		ownershipMap[curr.getData().getOwningInstance().instanceId] = ownership
		curr = curr.next
		if curr == head {
			break
		}
	}
	return verifyReplicaStartMap(t, tokenDistributor, replicaStartMap) && verifyOwnershipMap(t, tokenDistributor, ownershipMap)
}

func verifyReplicaStartMap(t *testing.T, tokenDistributor *TokenDistributor, replicaStartMap map[Token]Token) bool {
	success := true
	for _, token := range tokenDistributor.sortedTokens {
		replicaStart, err := tokenDistributor.replicationStrategy.getReplicaStart(token, tokenDistributor.sortedTokens, tokenDistributor.instanceByToken)
		require.Nil(t, err)

		if replicaStartMap[token] != replicaStart {
			fmt.Printf("Replica start of %d should be %d according to replica strategy but it is actually calculated as %d\n", token, replicaStart, replicaStartMap[token])
			success = false
		}

	}
	return success
}

func verifyOwnershipMap(t *testing.T, tokenDistributor *TokenDistributor, ownershipMap map[Instance]float64) bool {
	success := true
	for instance, tokens := range tokenDistributor.tokensByInstance {
		calculatedOwnership := 0.0
		for _, token := range tokens {
			replicaStart, err := tokenDistributor.replicationStrategy.getReplicaStart(token, tokenDistributor.sortedTokens, tokenDistributor.instanceByToken)
			require.Nil(t, err)
			indexFloor := searchTokenFloor(tokenDistributor.sortedTokens, replicaStart) - 1
			if indexFloor < 0 {
				indexFloor = len(tokenDistributor.sortedTokens) - 1
			}
			calculatedOwnership += float64(tokenDistributor.sortedTokens[indexFloor].distance(token, tokenDistributor.maxTokenValue))
		}
		if calculatedOwnership != ownershipMap[instance] {
			fmt.Printf("Ownership of %s should be %.2f according to replica strategy but it is actually calculated as %.2f\n", instance, calculatedOwnership, ownershipMap[instance])
			success = false
		}
	}
	return success
}
