package tokendistributor

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
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
	replicationFactor          = 3
)

func createTokenDistributor(maxTokenValue Token) *TokenDistributor {
	sortedRingTokens, ringInstanceByToken, zoneByInstance := createRingTokensInstancesZones()
	replicationStrategy := NewZoneAwareReplicationStrategy(zonesCount, zoneByInstance, nil, nil)
	tokenDistributor := newTokenDistributorFromInitializedInstances(sortedRingTokens, ringInstanceByToken, zonesCount, zoneByInstance, tokensPerInstance, replicationStrategy)
	tokenDistributor.maxTokenValue = maxTokenValue
	return tokenDistributor
}

func createTokenDistributorWithInitialEvenDistribution(start, maxTokenValue, tokensPerInstanceCount, zonesCount int) *TokenDistributor {
	sortedRingTokens, ringInstanceByToken, zoneByInstance := createRingTokensInstancesZonesEven50(start, maxTokenValue, tokensPerInstanceCount, zonesCount)
	replicationStrategy := NewZoneAwareReplicationStrategy(zonesCount, zoneByInstance, nil, nil)
	tokenDistributor := newTokenDistributorFromInitializedInstances(sortedRingTokens, ringInstanceByToken, zonesCount, zoneByInstance, tokensPerInstance, replicationStrategy)
	tokenDistributor.maxTokenValue = Token(maxTokenValue)
	return tokenDistributor
}

func createTokenInfoCircularList(tokenDistributor *TokenDistributor, newInstance *instanceInfo) *CircularList[*tokenInfo] {
	infoInstanceByInstance, _ := tokenDistributor.createInstanceAndZoneInfos()
	return tokenDistributor.createTokenInfoCircularList(infoInstanceByInstance, newInstance)
}

func createNewInstanceAncCircularListsWithVerification(t *testing.T, tokenDistributor *TokenDistributor, newInstance Instance, newInstanceZone Zone, verify bool) (*CircularList[*tokenInfo], *CircularList[*candidateTokenInfo]) {
	infoInstanceByInstance, zoneInfoByZone := tokenDistributor.createInstanceAndZoneInfos()
	optimalTokenOwnership := tokenDistributor.getOptimalTokenOwnership()
	newInstanceInfo := newInstanceInfo(newInstance, zoneInfoByZone[newInstanceZone], tokenDistributor.tokensPerInstance)
	newInstanceInfo.ownership = float64(tokenDistributor.tokensPerInstance) * optimalTokenOwnership
	tokenInfoCircularList := tokenDistributor.createTokenInfoCircularList(infoInstanceByInstance, newInstanceInfo)
	if verify {
		require.True(t, verifyReplicaStartAndReplicatedOwnership(t, tokenDistributor, tokenInfoCircularList))
	}
	candidateTokenInfoCircularList := tokenDistributor.createCandidateTokenInfoCircularList(tokenInfoCircularList, newInstanceInfo, optimalTokenOwnership)
	if verify {
		require.True(t, verifyReplicaStartAndReplicatedOwnership(t, tokenDistributor, tokenInfoCircularList))
	}
	return tokenInfoCircularList, candidateTokenInfoCircularList
}

func TestTokenDistributor_CreateTokenInfoCircularList(t *testing.T) {
	tokenDistributor := createTokenDistributor(maxToken)
	newInstanceInfo := newInstanceInfo(newInstance, newZoneInfo(newInstanceZone), tokensPerInstance)
	tokenInfoCircularList := createTokenInfoCircularList(tokenDistributor, newInstanceInfo)
	for _, token := range tokenDistributor.sortedTokens {
		head := tokenInfoCircularList.head
		fmt.Println(head.getData())
		require.Equal(t, head.getData().getToken(), token)
		tokenInfoCircularList.remove(head)
	}
}

func TestTokenDistributor_CalculateCandidateToken(t *testing.T) {
	tokenDistributor := createTokenDistributor(maxToken)
	tokenInfoCircularList, _ := createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, false)
	head := tokenInfoCircularList.head
	first := head.getData().getToken()
	second := head.getNext().getToken()
	expected := uint32(math.Ceil(float64(first+second) / 2.0))
	actual, err := tokenDistributor.calculateCandidateToken(head.data)
	require.Nil(t, err)
	require.Equal(t, expected, uint32(actual))
	tokenDistributor.sortedTokens = append(tokenDistributor.sortedTokens, actual)
	_, err = tokenDistributor.calculateCandidateToken(head.data)
	require.NotNil(t, err)

	tail := head.prev
	first = tail.getData().getToken()
	second = head.getData().getToken()
	actual, err = tokenDistributor.calculateCandidateToken(tail.data)
	expected = uint32(math.Ceil(float64(maxToken-first+second)/2.0)) + uint32(first)
	require.Nil(t, err)
	require.Equal(t, expected, uint32(actual))
	tokenDistributor.sortedTokens = append(tokenDistributor.sortedTokens, actual)
	_, err = tokenDistributor.calculateCandidateToken(tail.data)
	require.NotNil(t, err)
}

/*func TestTokenDistributor_CalculateCandidateToken(t *testing.T) {
	tokenDistributor := createTokenDistributor(maxToken)
	tokenInfoCircularList, _ := createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, false)
	first := 48
	second := 97
	delta := (first+second)/2 - first
	i := 0
	for {
		curr := tokenInfoCircularList.head
		actual, err := tokenDistributor.calculateCandidateToken(curr.data)
		if i < delta {
			tokenDistributor.sortedTokens = append(tokenDistributor.sortedTokens, actual)
			expected := uint32(math.Ceil(float64(first+second)/2.0)) + uint32(i)
			require.Equal(t, expected, uint32(actual))
		} else {
			require.NotNil(t, err)
			break
		}
		i++
	}
	first = 902
	second = 48
	middle := int(math.Ceil(float64(int(maxToken)-first+second)/2.0)) + first
	delta = int(maxToken) - middle + second + 1
	i = 0
	for {
		curr := tokenInfoCircularList.head.prev
		actual, err := tokenDistributor.calculateCandidateToken(curr.data)
		if i < delta {
			tokenDistributor.sortedTokens = append(tokenDistributor.sortedTokens, actual)
			expected := uint32(middle) + uint32(i)
			if expected > 1000 {
				expected -= 1001
			}
			require.Equal(t, expected, uint32(actual))
		} else {
			require.NotNil(t, err)
			break
		}
		i++
	}

}*/

func TestTokenDistributor_CreateCandidateTokenInfoCircularList(t *testing.T) {
	tokenDistributor := createTokenDistributor(maxToken)
	tokenInfoCircularList, candidateTokenCircularList := createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, false)

	curr1 := tokenInfoCircularList.head
	curr2 := candidateTokenCircularList.head
	for _ = range tokenDistributor.sortedTokens {
		candidateToken, _ := tokenDistributor.calculateCandidateToken(curr1.getData())
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

	oldReplicaSetMap, oldOwnership := createReplicaStartAndReplicatedOwnershipMaps(tokenDistributor, tokenInfoCircularList)

	bestCandidate := curr.getData()
	bestCandidate.getOwningInstance().ownership = 0
	next0 := tokenDistributor.calculateReplicatedOwnership(curr.getNext(), curr.getNext().getReplicaStart().(navigableTokenInterface))
	tokenDistributor.addCandidateToTokenInfoCircularList(curr, tokenInfoCircularList)
	next1 := tokenDistributor.calculateReplicatedOwnership(curr.getNext(), curr.getNext().getReplicaStart().(navigableTokenInterface))
	fmt.Println(next1, next0)

	first := tokenInfoCircularList.head
	currTokenInfo := first.next
	for ; currTokenInfo != first; currTokenInfo = currTokenInfo.next {
		currToken := currTokenInfo.getData().getToken()
		currInstance := currTokenInfo.getData().getOwningInstance()
		diff := Token(736).distance(Token(770), tokenDistributor.maxTokenValue)
		switch currToken {
		case Token(770):
			require.Equal(t, Token(736), currTokenInfo.getPrev().getToken())
			require.Equal(t, Token(804), currTokenInfo.getNext().getToken())
			require.Equal(t, Token(770), currTokenInfo.getData().getReplicaStart().getToken())
			require.Equal(t, float64(Token(736).distance(Token(770), tokenDistributor.maxTokenValue)), currTokenInfo.getData().getReplicatedOwnership())
			require.Equal(t, float64(diff), currInstance.ownership)
		case Token(853):
			require.Equal(t, Token(804), currTokenInfo.getPrev().getToken())
			require.Equal(t, Token(902), currTokenInfo.getNext().getToken())
			require.Equal(t, Token(804), currTokenInfo.getData().getReplicaStart().getToken())
			require.Equal(t, float64(Token(770).distance(Token(853), tokenDistributor.maxTokenValue)), currTokenInfo.getData().getReplicatedOwnership())
			require.Equal(t, oldOwnership[currInstance.instanceId]-float64(diff), currInstance.ownership)
		default:
			require.Equal(t, oldReplicaSetMap[currToken], currTokenInfo.getData().getReplicaStart().getToken())
			if currInstance.zone.zone != newInstanceZone {
				require.Equal(t, oldOwnership[currInstance.instanceId], currInstance.ownership)
			} else {
				require.Equal(t, oldOwnership[currInstance.instanceId]-float64(diff), currInstance.ownership)
			}
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
	tokenList, candidateList, _ := tokenDistributor.AddInstance(newInstance, newInstanceZone)
	fmt.Println(tokenDistributor.sortedTokens)
	fmt.Println(tokenList)
	fmt.Println(candidateList)
}

func TestTokenDistributor_AddInstanceInitialEvenDistribution(t *testing.T) {
	tokenDistributor := createTokenDistributorWithInitialEvenDistribution(50, 1200, tokensPerInstance, zonesCount)
	tokenCircularList, candidateTokenCircularList := createNewInstanceAncCircularListsWithVerification(t, tokenDistributor, newInstance, newInstanceZone, false)
	fmt.Println(tokenCircularList)
	fmt.Println(candidateTokenCircularList)
	tokenList, candidateList, _ := tokenDistributor.AddInstance(newInstance, newInstanceZone)
	fmt.Println(tokenDistributor.sortedTokens)
	fmt.Println(tokenList)
	fmt.Println(candidateList)
}

func TestTokenDistributor_AddFirstInstanceOfAZone(t *testing.T) {
	zones := []Zone{"zone-a", "zone-b", "zone-c"}
	replicationStrategy := NewZoneAwareReplicationStrategy(replicationFactor, make(map[Instance]Zone, initialInstanceCount), nil, nil)
	tokenDistributor := NewTokenDistributor(tokensPerInstance, zonesCount, maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))
	instances := []Instance{"A", "B", "C"}

	for i := range instances {
		require.Len(t, tokenDistributor.sortedTokens, i*tokensPerInstance)
		_, ok := tokenDistributor.tokensByInstance[instances[i]]
		require.NotNil(t, ok)
		tokenDistributor.AddInstance(instances[i], zones[i])
		require.Len(t, tokenDistributor.sortedTokens, (i+1)*tokensPerInstance)
		_, ok = tokenDistributor.tokensByInstance[instances[i]]
		require.True(t, ok)
		slices.IsSorted(tokenDistributor.sortedTokens)
	}
}

func TestTokenDistributor_AddSecondInstanceOfAZone(t *testing.T) {
	zones := []Zone{"zone-a", "zone-b", "zone-c"}
	replicationStrategy := NewZoneAwareReplicationStrategy(replicationFactor, make(map[Instance]Zone, initialInstanceCount), nil, nil)
	tokenDistributor := NewTokenDistributor(tokensPerInstance, zonesCount, maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))
	tokenDistributor.maxTokenValue = maxToken
	instances := []Instance{"A-1", "B-1", "C-1"}

	for i := range instances {
		tokenDistributor.AddInstance(instances[i], zones[i])
	}
	instances = []Instance{"A-2", "B-2", "C-2", "A-3", "B-3", "C-3"}
	for i := range instances {
		tokenDistributor.AddInstance(instances[i], zones[i%len(zones)])
	}

	require.Len(t, tokenDistributor.sortedTokens, 9*tokensPerInstance)
}

func TestTokenDistributor_GenerationZoneAware(t *testing.T) {
	iterations := 3
	zones := []Zone{"zone-a", "zone-b", "zone-c"}
	replicationFactor := len(zones)
	maxToken := Token(math.MaxUint32)
	numberOfInstancesPerZone := 30
	tokensPerInstance := 512
	stats := make([]Statistics, 0, iterations)

	for it := 0; it < iterations; it++ {
		fmt.Printf("Iteration %d...\n", it+1)
		replicationStrategy := NewZoneAwareReplicationStrategy(replicationFactor, make(map[Instance]Zone, initialInstanceCount), nil, nil)
		tokenDistributor := NewTokenDistributor(tokensPerInstance, len(zones), maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))

		for i := 0; i < numberOfInstancesPerZone; i++ {
			for j := 0; j < len(zones); j++ {
				instance := Instance(fmt.Sprintf("%s-%d", string(rune('A'+j)), i))
				_, _, stat := tokenDistributor.AddInstance(instance, zones[j])
				fmt.Printf("Instance %s added\n", instance)
				stats = append(stats, stat)
			}
		}
		require.Len(t, tokenDistributor.sortedTokens, len(zones)*tokensPerInstance*numberOfInstancesPerZone)
	}
	statistics := GetAverageStatistics(stats)
	statistics.Print()
}

func TestTokenDistributor_GenerationNoReplication(t *testing.T) {
	iterations := 10
	replicationFactor := 1
	maxToken := Token(math.MaxUint32)
	zones := []Zone{SingleZone}
	numberOfInstancesPerZone := 20
	tokensPerInstance := 64
	stats := make([]Statistics, 0, iterations)

	for it := 0; it < iterations; it++ {
		fmt.Printf("Iteration %d...\n", it+1)
		replicationStrategy := newSimpleReplicationStrategy(replicationFactor, nil)
		tokenDistributor := NewTokenDistributor(tokensPerInstance, len(zones), maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))

		for i := 0; i < numberOfInstancesPerZone; i++ {
			instance := Instance(fmt.Sprintf("instance-%d", i))
			_, _, stat := tokenDistributor.AddInstance(instance, SingleZone)
			fmt.Printf("Instance %s added\n", instance)
			stats = append(stats, stat)
		}
		require.Len(t, tokenDistributor.sortedTokens, tokensPerInstance*numberOfInstancesPerZone)
	}
	statistics := GetAverageStatistics(stats)
	statistics.Print()
}

func TestTokenDistributor_GenerationReplicationWithoutZones(t *testing.T) {
	iterations := 10
	replicationFactor := 3
	maxToken := Token(math.MaxUint32)
	zones := []Zone{SingleZone}
	numberOfInstancesPerZone := 66
	tokensPerInstance := 64
	stats := make([]Statistics, 0, iterations)

	for it := 0; it < iterations; it++ {
		fmt.Printf("Iteration %d...\n", it+1)
		replicationStrategy := newSimpleReplicationStrategy(replicationFactor, nil)
		tokenDistributor := NewTokenDistributor(tokensPerInstance, len(zones), maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))

		for i := 0; i < numberOfInstancesPerZone; i++ {
			instance := Instance(fmt.Sprintf("instance-%d", i))
			_, _, stat := tokenDistributor.AddInstance(instance, SingleZone)
			fmt.Printf("Instance %s added\n", instance)
			stats = append(stats, stat)
		}
		require.Len(t, tokenDistributor.sortedTokens, tokensPerInstance*numberOfInstancesPerZone)
	}
	statistics := GetAverageStatistics(stats)
	statistics.Print()
}

func TestTokenDistributor_GenerationZoneAwareWithTokens(t *testing.T) {
	zones := []Zone{"zone-a", "zone-b", "zone-c"}
	replicationFactor := len(zones)
	maxToken := Token(math.MaxUint32)
	numberOfInstancesPerZone := 22
	tokensPerInstance := 64

	replicationStrategy := NewZoneAwareReplicationStrategy(replicationFactor, make(map[Instance]Zone, initialInstanceCount), nil, nil)
	tokenDistributor := NewTokenDistributor(tokensPerInstance, len(zones), maxToken, replicationStrategy, NewPerfectlySpacedSeedGenerator(zones, replicationFactor, tokensPerInstance, maxToken))

	var stat Statistics
	for i := 0; i < numberOfInstancesPerZone; i++ {
		for j := 0; j < len(zones); j++ {
			instance := Instance(fmt.Sprintf("%s-%d", string(rune('A'+j)), i))
			_, _, stat = tokenDistributor.AddInstance(instance, zones[j])
		}
	}
	stat.Print()

	replicatedOwnership := make(map[Instance]int, len(tokenDistributor.tokensByInstance))
	timeseriesCount := 1000000
	for i := 0; i < timeseriesCount; i++ {
		token := Token(rand.Uint32())
		replicaSet, err := replicationStrategy.getReplicaSet(token, tokenDistributor.sortedTokens, tokenDistributor.instanceByToken)
		if err != nil {
			panic(err)
		}
		for _, instance := range replicaSet {
			ownership, ok := replicatedOwnership[instance]
			if !ok {
				ownership = 0
			}
			ownership++
			replicatedOwnership[instance] = ownership
		}
	}

	fmt.Println(replicatedOwnership)
	optimalTimeseriesOwnership := float64(timeseriesCount*replicationFactor) / float64(len(tokenDistributor.sortedTokens))
	fmt.Println(optimalTimeseriesOwnership, optimalTimeseriesOwnership*float64(tokensPerInstance))
	stat = getTimeseriesStatistics(tokenDistributor, replicatedOwnership, optimalTimeseriesOwnership, timeseriesCount)
	stat.Print()
}
