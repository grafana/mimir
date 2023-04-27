package tokendistributor

import (
	"container/heap"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

const (
	tokensPerInstance          = 4
	zonesCount                 = 3
	newInstanceZone   Zone     = "zone-b"
	newInstance       Instance = "instance-4"
	maxToken          Token    = 1000
)

func createTokenDistributor(maxTokenValue Token) *TokenDistributor {
	sortedRingTokens, ringInstanceByToken, zoneByInstance := createRingTokensInstancesZones()
	replicationStrategy := newZoneAwareReplicationStrategy(zonesCount, zoneByInstance, nil, nil)
	tokenDistributor := newTokenDistributorFromInitializedInstances(sortedRingTokens, ringInstanceByToken, tokensPerInstance, replicationStrategy)
	tokenDistributor.maxTokenValue = maxTokenValue
	return tokenDistributor
}

func createTokenDistributorWithInitialEvenDistribution(start, maxTokenValue, tokensPerInstanceCount, zonesCount int) *TokenDistributor {
	sortedRingTokens, ringInstanceByToken, zoneByInstance := createRingTokensInstancesZonesEven50(start, maxTokenValue, tokensPerInstanceCount, zonesCount)
	replicationStrategy := newZoneAwareReplicationStrategy(zonesCount, zoneByInstance, nil, nil)
	tokenDistributor := newTokenDistributorFromInitializedInstances(sortedRingTokens, ringInstanceByToken, tokensPerInstance, replicationStrategy)
	tokenDistributor.maxTokenValue = Token(maxTokenValue)
	return tokenDistributor
}

func createTokenInfoCircularList(tokenDistributor *TokenDistributor, newInstanceZone Zone) *CircularList[*tokenInfo] {
	infoInstanceByInstance, zoneInfoByZone := tokenDistributor.createInstanceAndZoneInfos()
	return tokenDistributor.createTokenInfoCircularList(infoInstanceByInstance, zoneInfoByZone[newInstanceZone])
}

func createNewInstanceAncCircularListsWithVerification(t *testing.T, tokenDistributor *TokenDistributor, newInstance Instance, newInstanceZone Zone, verify bool) (*CircularList[*tokenInfo], *CircularList[*candidateTokenInfo]) {
	infoInstanceByInstance, zoneInfoByZone := tokenDistributor.createInstanceAndZoneInfos()
	tokenInfoCircularList := tokenDistributor.createTokenInfoCircularList(infoInstanceByInstance, zoneInfoByZone[newInstanceZone])
	if verify {
		require.True(t, verifyReplicaStartAndReplicatedOwnership(t, tokenDistributor, tokenInfoCircularList))
	}
	optimalTokenOwnership := tokenDistributor.getOptimalTokenOwnership()
	newInstanceInfo := newInstanceInfo(newInstance, zoneInfoByZone[newInstanceZone], tokenDistributor.tokensPerInstance)
	newInstanceInfo.ownership = float64(tokenDistributor.tokensPerInstance) * optimalTokenOwnership
	candidateTokenInfoCircularList := tokenDistributor.createCandidateTokenInfoCircularList(tokenInfoCircularList, newInstanceInfo, optimalTokenOwnership)
	if verify {
		require.True(t, verifyReplicaStartAndReplicatedOwnership(t, tokenDistributor, tokenInfoCircularList))
	}
	return tokenInfoCircularList, candidateTokenInfoCircularList
}

func TestTokenDistributor_CreateTokenInfoCircularList(t *testing.T) {
	tokenDistributor := createTokenDistributor(maxToken)
	tokenInfoCircularList := createTokenInfoCircularList(tokenDistributor, newInstanceZone)
	for _, token := range tokenDistributor.sortedTokens {
		head := tokenInfoCircularList.head
		fmt.Println(head.getData())
		require.Equal(t, head.getData().getToken(), token)
		tokenInfoCircularList.remove(head)
	}
}

func TestTokenDistributor_CreateCandidateTokenInfoCircularList(t *testing.T) {
	tokenDistributor := createTokenDistributor(maxToken)
	tokenInfoCircularList, candidateTokenCircularList := createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, false)

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
	tokenDistributor := createTokenDistributor(maxToken)
	_, candidateTokenCircularList := createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, false)
	optimalTokenOwnership := tokenDistributor.getOptimalTokenOwnership()
	head := candidateTokenCircularList.head
	curr := head
	bestOwnershipDecrease := math.MaxFloat64
	bestCandidate := head
	for {
		candidate := curr.getData()
		improvement := tokenDistributor.evaluateImprovement(candidate, optimalTokenOwnership, 1/float64(tokensPerInstance))
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
	tokenDistributor := createTokenDistributor(maxToken)
	_, candidateTokenCircularList := createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, false)
	optimalTokenOwnership := tokenDistributor.getOptimalTokenOwnership()

	sortedImprovements := make([]float64, 0, len(tokenDistributor.sortedTokens))
	head := candidateTokenCircularList.head
	curr := head
	for {
		candidate := curr.getData()
		improvement := tokenDistributor.evaluateImprovement(candidate, optimalTokenOwnership, 1/float64(tokensPerInstance))
		sortedImprovements = append(sortedImprovements, improvement)
		curr = curr.next
		if curr == head {
			break
		}
	}

	slices.SortFunc(sortedImprovements, func(a, b float64) bool {
		return b < a
	})

	pq := tokenDistributor.createPriorityQueue(candidateTokenCircularList, optimalTokenOwnership, tokensPerInstance)
	for _, improvement := range sortedImprovements {
		improvementFromPQ := heap.Pop(pq).(*WeightedNavigableToken[*candidateTokenInfo]).weight
		require.Equal(t, improvement, improvementFromPQ)
	}
}

func TestTokenDistributor_VerifyTokenInfo(t *testing.T) {
	tokenDistributor := createTokenDistributor(maxToken)
	createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, true)
}

func TestTokenDistributor_AddCandidateToTokenInfoCircularList(t *testing.T) {
	tokenDistributor := createTokenDistributor(maxToken)
	tokenInfoCircularList, candidateTokenCircularList := createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, false)

	// find candidate with token 770
	head := candidateTokenCircularList.head
	curr := head
	for {
		curr = curr.next
		if curr.getData().getToken() == Token(770) || curr == head {
			break
		}
	}
	require.NotEqual(t, head, curr)

	oldReplicaSetMap, _ := createReplicaStartAndReplicatedOwnershipMaps(tokenDistributor, tokenInfoCircularList)

	tokenDistributor.addCandidateToTokenInfoCircularList(curr.getData())
	verifyReplicaStartAndReplicatedOwnership(t, tokenDistributor, tokenInfoCircularList)

	first := tokenInfoCircularList.head
	currTokenInfo := first.next
	for ; currTokenInfo != first; currTokenInfo = currTokenInfo.next {
		currToken := currTokenInfo.getData().getToken()
		switch currToken {
		case Token(770):
			require.Equal(t, Token(736), currTokenInfo.getPrev().getToken())
			require.Equal(t, Token(804), currTokenInfo.getNext().getToken())
			require.Equal(t, Token(770), currTokenInfo.getData().getReplicaStart().getToken())
			require.Equal(t, float64(Token(736).distance(Token(770), tokenDistributor.maxTokenValue)), currTokenInfo.getData().getReplicatedOwnership())
		case Token(853):
			require.Equal(t, Token(804), currTokenInfo.getPrev().getToken())
			require.Equal(t, Token(902), currTokenInfo.getNext().getToken())
			require.Equal(t, Token(804), currTokenInfo.getData().getReplicaStart().getToken())
			require.Equal(t, float64(Token(770).distance(Token(853), tokenDistributor.maxTokenValue)), currTokenInfo.getData().getReplicatedOwnership())
		default:
			require.Equal(t, oldReplicaSetMap[currToken], currTokenInfo.getData().getReplicaStart().getToken())
		}
	}
}

func createReplicaStartAndReplicatedOwnershipMaps(tokenDistributor *TokenDistributor, tokenInfoCircularList *CircularList[*tokenInfo]) (map[Token]Token, map[Instance]float64) {
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
	return replicaStartMap, ownershipMap
}

func verifyReplicaStartAndReplicatedOwnership(t *testing.T, tokenDistributor *TokenDistributor, tokenInfoCircularList *CircularList[*tokenInfo]) bool {
	replicaStartMap, ownershipMap := createReplicaStartAndReplicatedOwnershipMaps(tokenDistributor, tokenInfoCircularList)
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

func TestTokenDistributor_NonSoQuale(t *testing.T) {
	tokenDistributor := createTokenDistributorWithInitialEvenDistribution(50, 1200, tokensPerInstance, zonesCount)
	tokenCircularList, candidateTokenCircularList := createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, false)
	fmt.Println(tokenCircularList)
	fmt.Println(candidateTokenCircularList)
	optimalTokenOwnership := tokenDistributor.getOptimalTokenOwnership()
	head := candidateTokenCircularList.head
	curr := head
	bestOwnershipDecrease := math.MaxFloat64
	bestCandidate := head
	for {
		candidate := curr.getData()
		improvement := tokenDistributor.evaluateImprovement(candidate, optimalTokenOwnership, 1/float64(tokensPerInstance))
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

	fmt.Printf("Best candidate is %d\n", bestCandidate.getData().getToken())
}

func TestTokenDistributor_AddInstance(t *testing.T) {
	tokenDistributor := createTokenDistributor(maxToken)
	tokenCircularList, candidateTokenCircularList := createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, false)
	fmt.Println(tokenCircularList)
	fmt.Println(candidateTokenCircularList)
	tokens, list := tokenDistributor.AddInstance(newInstance, newInstanceZone)
	fmt.Println(tokens)
	fmt.Println(list)
}
