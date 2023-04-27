package tokendistributor

import (
	"container/heap"
	"fmt"
	"math"
	"strings"

	"golang.org/x/exp/slices"
)

const (
	initialInstanceCount = 66
)

type TokenDistributor struct {
	sortedTokens        []Token
	instanceByToken     map[Token]Instance
	replicationStrategy ReplicationStrategy
	tokensPerInstance   int
	tokensByInstance    map[Instance][]Token
	maxTokenValue       Token
	zoneByInstance      map[Instance]Zone
	zones               []Zone
	seedByZone          map[Zone][]Token
}

func newTokenDistributor(tokensPerInstance, zonesCount int, maxTokenValue Token, replicationStrategy ReplicationStrategy, seedGenerator SeedGenerator) *TokenDistributor {
	sortedTokens := make([]Token, 0, initialInstanceCount*tokensPerInstance)
	instanceByToken := make(map[Token]Instance, initialInstanceCount*tokensPerInstance)
	tokensByInstance := make(map[Instance][]Token, initialInstanceCount)
	zoneByInstance := make(map[Instance]Zone, initialInstanceCount)

	tokenDistributor := &TokenDistributor{
		sortedTokens:        sortedTokens,
		instanceByToken:     instanceByToken,
		zoneByInstance:      zoneByInstance,
		zones:               generateZones(zonesCount),
		replicationStrategy: replicationStrategy,
		tokensByInstance:    tokensByInstance,
		maxTokenValue:       maxTokenValue,
		tokensPerInstance:   tokensPerInstance,
	}
	tokenDistributor.seedByZone = seedGenerator.generateSeedByZone(tokenDistributor.zones, tokenDistributor.tokensPerInstance, tokenDistributor.maxTokenValue)
	return tokenDistributor
}

// newTokenDistributorFromInitializedInstances supposes that the ring already contains some instances with tokensPerInstance tokens each
func newTokenDistributorFromInitializedInstances(sortedTokens []Token, instanceByToken map[Token]Instance, zonesCount int, zoneByInstance map[Instance]Zone, tokensPerInstance int, replicationStrategy ReplicationStrategy) *TokenDistributor {
	tokensByInstance := make(map[Instance][]Token, len(sortedTokens)/tokensPerInstance)
	for token, instance := range instanceByToken {
		tokens, ok := tokensByInstance[instance]
		if !ok {
			tokens = make([]Token, 0, tokensPerInstance)
		}
		tokens = append(tokens, token)
		tokensByInstance[instance] = tokens
	}
	tokens := make([]Token, 0, initialInstanceCount*tokensPerInstance)
	for _, token := range sortedTokens {
		tokens = append(tokens, token)
	}
	return &TokenDistributor{
		sortedTokens:        tokens,
		instanceByToken:     instanceByToken,
		zoneByInstance:      zoneByInstance,
		zones:               generateZones(zonesCount),
		replicationStrategy: replicationStrategy,
		tokensPerInstance:   tokensPerInstance,
		tokensByInstance:    tokensByInstance,
		maxTokenValue:       maxTokenValue,
	}
}

func generateZones(zonesCount int) []Zone {
	zones := make([]Zone, 0, zonesCount)
	for i := 0; i < zonesCount; i++ {
		zones = append(zones, Zone("zone-"+string(rune('a'+i))))
	}
	return zones
}

func (t *TokenDistributor) addTokenToInstance(instance Instance, token Token) {
	tokens, ok := t.tokensByInstance[instance]
	if !ok {
		tokens = make([]Token, 0, t.tokensPerInstance)
	}
	tokens = append(tokens, token)
	t.tokensByInstance[instance] = tokens
}

func (t *TokenDistributor) getInstanceCount() int {
	return len(t.tokensByInstance)
}

func (t *TokenDistributor) getOptimalTokenOwnership() float64 {
	maxTokenAsFloat64 := float64(t.maxTokenValue)
	return maxTokenAsFloat64 * float64(t.replicationStrategy.getReplicationFactor()) / float64(len(t.sortedTokens)+t.tokensPerInstance)
}

// calculateReplicatedOwnership calculated the replicated weight of the given token with its given replicaStart.
// The replicated weight is basically the distance between replicaStart's predecessor and the token itself.
func (t *TokenDistributor) calculateReplicatedOwnership(token, replicaStart navigableTokenInterface) float64 {
	return t.calculateDistanceAsFloat64(replicaStart.getPrevious(), token)
}

// calculateReplicatedOwnership calculated the replicated weight of the given token with its given replicaStart.
// The replicated weight is basically the distance between replicaStart's predecessor and the token itself.
func (t *TokenDistributor) calculateDistanceAsFloat64(first, second navigableTokenInterface) float64 {
	return float64(first.getToken().distance(second.getToken(), t.maxTokenValue))
}

func (t *TokenDistributor) createInstanceAndZoneInfos() (map[Instance]*instanceInfo, map[Zone]*zoneInfo) {
	zoneInfoByZone := make(map[Zone]*zoneInfo)
	instanceInfoByInstance := make(map[Instance]*instanceInfo, len(t.tokensByInstance))
	for instance, tokens := range t.tokensByInstance {
		zone := t.replicationStrategy.getZone(instance)
		zoneInfoByZone[zone] = newZoneInfo(zone)
		instanceInfoByInstance[instance] = newInstanceInfo(instance, zoneInfoByZone[zone], len(tokens))
	}
	return instanceInfoByInstance, zoneInfoByZone
}

// createTokenInfoCircularList creates a circularList whose elements are of type tokenInfo.
// The resulting circularList is created starting from the slice of sorted tokens sortedTokens, and represent the actual
// ring. For tokenInfo its replica start (tokenInfo.replicaStart) and expandable (tokenInfo.expandable) are calculated
// and stored
func (t *TokenDistributor) createTokenInfoCircularList(instanceInfoByInstance map[Instance]*instanceInfo, newInstanceZone *zoneInfo) *CircularList[*tokenInfo] {
	circularList := newCircularList[*tokenInfo]()
	for _, token := range t.sortedTokens {
		instanceInfo := instanceInfoByInstance[t.instanceByToken[token]]
		navigableToken := newNavigableTokenInfo(newTokenInfo(instanceInfo, token))
		circularList.insertLast(navigableToken)
	}

	curr := circularList.head
	for _ = range t.sortedTokens {
		t.populateTokenInfo(curr.getData(), newInstanceZone)
		curr = curr.next
	}

	return &circularList
}

// createCandidateTokenInfoCircularList creates a circularList whose elements are of type candidateTokenInfo.
// The resulting circularList is created starting from a circularList of tokenInfo elements in such a way that between
// each 2 tokenInfos belonging to the former, a candidateTokenInfo is created in the middle of the 2 tokenInfos.
func (t *TokenDistributor) createCandidateTokenInfoCircularList(tokenInfoCircularList *CircularList[*tokenInfo], newInstanceInfo *instanceInfo, initialTokenOwnership float64) *CircularList[*candidateTokenInfo] {
	circularList := newCircularList[*candidateTokenInfo]()
	curr := tokenInfoCircularList.head
	for {
		currentTokenInfo := curr.getData()
		candidateToken := t.calculateCandidateToken(currentTokenInfo)
		candidateTokenInfo := newCandidateTokenInfo(newInstanceInfo, candidateToken, currentTokenInfo)
		candidateTokenInfo.setReplicatedOwnership(initialTokenOwnership)
		navigableToken := newNavigableCandidateTokenInfo(candidateTokenInfo)
		circularList.insertLast(navigableToken)
		t.populateCandidateTokenInfo(navigableToken.getData(), newInstanceInfo.zone)

		curr = curr.next
		if curr == tokenInfoCircularList.head {
			break
		}
	}
	return &circularList
}

func (t *TokenDistributor) calculateCandidateToken(tokenInfo *tokenInfo) Token {
	currentToken := tokenInfo.getToken()
	nextToken := tokenInfo.getNext().getToken()
	return currentToken.split(nextToken, t.maxTokenValue)
}

// calculateReplicaStartAndExpansion crosses the ring backwards starting from the token corresponding to navigableToken,
// and looks for the first token belonging to the same zone. The token preceding the latter is a replica start of
// navigableToken, and is returned as a result of calculateReplicaStartAndExpansion.
// Another result returned by calculateReplicaStartAndExpansion is whether the replica set starting in the first result
// of the function could be expanded by a token belonging to newInstanceZone. Expansion is possible in the following
// cases:
// 1) if the replica set is not full (i.e., if tokens between navigableToken and its replica set do not cover all the
// zones) and the replica start is not in newInstanceZone (e.g., "zone-a"->"zone-c"->"zone-a" is expandable by "zone-b",
// but "zone-b"->"zone-c"->"zone-b" is not expandable by "zone-b" because that expansion would actually give raise to
// 2 replica sets "zone-b"->"zone-c" and "zone-b"->"zone-b", or
// 2) if the replica set is not full and if it contains any token from newInstanceZone in the middle (e.g., replica sets
// "zone-a"->"zone-b"->"zone-a" and "zone-a"->"zone-c"->"zone-a" can be extended by "zone-b", or
// 3) if the replica set is full, but it neither starts nor ends with a token from newInstanceZone (e.g., replica sets
// "zone-a"->"zone-b"->"zone-c" and "zone-c"-"zone-b"-"zone-a" can be extended by "zone-b", but replica sets
// "zone-b"->"zone-a"->"zone-c" and "zone-a"->"zone-c"->"zone-b" cannot be extended by "zone-b"
func (t *TokenDistributor) calculateReplicaStartAndExpansion(navigableToken navigableTokenInterface, newInstanceZone *zoneInfo) (navigableTokenInterface, bool) {
	var (
		navigableTokenZone = navigableToken.getOwningInstance().zone
		prevZoneInfo       = &LastZoneInfo
		diffZonesCount     = 0
		expandable         = false
		replicationFactor  = t.replicationStrategy.getReplicationFactor()
		// replicaStart is the farthest away token before a token from the same zone is found
		replicaStart = navigableToken
		curr         = navigableToken.getPrevious()
	)

	// we are visiting tokens in "backwards" mode, i.e., by going towards the replica start
	for {
		currZone := curr.getOwningInstance().zone
		if currZone.precededBy == nil {
			// if currZone has not been visited
			// we mark zone as visited
			currZone.precededBy = prevZoneInfo
			prevZoneInfo = currZone
			diffZonesCount++
			if diffZonesCount == replicationFactor {
				// we have obtained the full replica
				break
			}

			// if we find an instance belonging to the same zone as tokenInfo, we have to break
			if currZone == navigableTokenZone {
				// inserting a new token from newInstanceZone at this boundary expands the replica set size,
				// but only if the current token is not from that zone
				// e.g., if navigableTokenZone is zone-a, and we are trying to place a token from zone-b (newInstanceZone),
				// a partial replica set "zone-a"->"zone-c"->"zone-a" can be extended by zone-b , obtaining replica set
				// "zone-b"->"zone-a"->"zone-b".
				// On the other hand, a partial replica set "zone-b"->"zone-c"->"zone-b" cannot be extended by "zone-b"
				// because that extension would split the replica in 2 different replicas
				expandable = currZone != newInstanceZone
				break
			}

			// it is possible to expand the replica set size by inserting a token of newInstanceZone at
			// replicaStart if there is another token from that zone in the middle
			if currZone == newInstanceZone {
				expandable = true
			}
		}
		replicaStart = curr
		curr = curr.getPrevious()
	}

	// clean up visited zones
	currZoneInfo := prevZoneInfo
	for currZoneInfo != &LastZoneInfo {
		prevZoneInfo = currZoneInfo.precededBy
		currZoneInfo.precededBy = nil
		currZoneInfo = prevZoneInfo
	}

	return replicaStart, expandable
}

// populateTokenInfo gets as input a tokenInfo, and it calculates and updates its replica start and expandable information
// in the ring. Moreover, it updates the replicated weight information of both token itself and the instance that
// corresponds to that node.
func (t *TokenDistributor) populateTokenInfo(tokenInfo *tokenInfo, newInstanceZone *zoneInfo) {
	replicaStart, expandable := t.calculateReplicaStartAndExpansion(tokenInfo, newInstanceZone)
	tokenInfo.setReplicaStart(replicaStart)
	tokenInfo.setExpandable(expandable)
	newOwnership := t.calculateReplicatedOwnership(tokenInfo, replicaStart)
	oldOwnership := tokenInfo.getReplicatedOwnership()
	tokenInfo.setReplicatedOwnership(newOwnership)
	tokenInfo.getOwningInstance().ownership += newOwnership - oldOwnership
}

// populateCandidateTokenInfo gets as input a candidateTokenInfo, it calculates and sets its replica start and expandable
// information starting from the position in the ring that corresponds to candidateTokenInfo's host, i.e., the token after
// which the candidate is being added to the ring
func (t *TokenDistributor) populateCandidateTokenInfo(candidateTokenInfo *candidateTokenInfo, newInstanceZone *zoneInfo) {
	replicaStart, expandable := t.calculateReplicaStartAndExpansion(candidateTokenInfo, newInstanceZone)
	candidateTokenInfo.setReplicaStart(replicaStart)
	candidateTokenInfo.setExpandable(expandable)
}

func (t *TokenDistributor) evaluateImprovement(candidate *candidateTokenInfo, optimalTokenOwnership, multiplier float64) float64 {
	newInstance := candidate.getOwningInstance()
	host := candidate.host
	next := host.getNext()
	improvement := 0.0

	// We first calculate the replication weight of candidate
	oldOwnership := candidate.getReplicatedOwnership()
	newOwnership := t.calculateReplicatedOwnership(candidate, candidate.getReplicaStart().(navigableTokenInterface))
	stdev := calculateImprovement(optimalTokenOwnership, newOwnership, oldOwnership, newInstance.tokenCount)
	improvement += stdev
	newInstance.adjustedOwnership = newInstance.ownership + newOwnership - oldOwnership
	stats := strings.Builder{}
	stats.WriteString(fmt.Sprintf("Adding candidate token %d (delta: %.3f, impr: %.5f) affects the following tokens:\n",
		candidate.getToken(), newOwnership/optimalTokenOwnership, stdev))

	// We now calculate potential changes in the replicated weight of the tokenInfo that succeeds the host (i.e., next).
	// This tokenInfo (next) is of interest because candidate has been placed between host and next.

	// On the other hand. the replicated weight of next can change

	// The replicated weight of host and tokenInfos preceding it cannot be changed because they precede candidate,
	// and the replicated weight is the distance between the (further) replica start of a token and the token itself.
	// Therefore, we cross the ring backwards starting from the tokenInfo that succeeds the host (i.e., next) and calculate
	// potential improvements of all the tokenInfos that could be potentially affected by candidate's insertion. This
	// basically means that we are looking for all the tokenInfos that replicate the candidate.
	// We keep track of visited zones, so that we don't count the same zone more than once. Once the count of visited
	// zones is equal to replicationStrategy.getReplicationFactor(), we can stop the visit. The reason for that is that
	// no other token that precedes the token of the last visited zone could contain a replica of candidate because
	// between that token (included) and candidate (excluded) there would be all available zones, and therefore candidate
	// would have already been replicated replicationStrategy.getReplicationFactor() times. In other words, the replica
	// start of that token would succeed candidate on the ring.
	//

	newInstanceZone := newInstance.zone
	//newInstance.precededBy = newInstance
	prevInstance := newInstance
	visitedZonesCount := 0
	for currToken := next; visitedZonesCount < t.replicationStrategy.getReplicationFactor(); currToken = currToken.getNext() {
		currInstance := currToken.getOwningInstance()
		currZone := currInstance.zone
		// if this zone has already been visited, we go on
		if currZone.precededBy != nil {
			continue
		}
		// otherwise we mark this zone as visited and increase the counter of visited zones
		currZone.precededBy = currZone
		visitedZonesCount++

		// If this is the firs time we encounter an instance, we initialize its adjustedOwnership and mark it as seen.
		// The adjustedOwnership of each node will be used for the final evaluation of weight, together with all
		// potential improvements of all visited tokens
		if currInstance.precededBy == nil {
			currInstance.adjustedOwnership = currInstance.ownership
			currInstance.precededBy = prevInstance
			prevInstance = currInstance
		}

		//newReplicaStart := currToken.getReplicaStart()
		barrier := currToken.getReplicaStart().(navigableTokenInterface).getPrevious()
		if currToken.isExpandable() {
			if currToken.getReplicaStart() == next {
				// If currToken is expandable (wrt. candidate's zone), then it is possible to extend its replica set by adding
				// a token from  candidate's zone as a direct predecessor of currToken replica start. Token candidate is being
				// evaluated as the direct predecessor of token next, and therefore only that case would be valid here. In this
				// case candidate would become the new replica start of currToken.
				//newReplicaStart = candidate
				barrier = host
			} else {
				continue
			}
		} else if currZone == newInstanceZone {
			if currToken.getReplicatedOwnership() > t.calculateDistanceAsFloat64(candidate, currToken) {
				// Barrier of a token is the first token belonging to the same zone of the former by crossing the ring
				// backwards, i.e., it is the direct predecessor of replica start of that token.
				// If currToken and candidate belong to the same zone, the only change that could happen here is if
				// candidate becomes a new barrier of currToken, and this is possible only if the current barrier of
				// currentToken precedes candidate.
				// Proof: if the current barrier of currentToken succeeds candidate, candidate would be at least the
				// second occurrence of the zone of currentToken, which contradicts the definition of term barrier.
				// In order to check whether the current barrier precedes candidate, we could compare the current
				// replicated weight of currToken (i.e., the range of tokens from its barrier to curentToken itself)
				// with the range of tokens from candidate to currentToken itself, and if the former is greater, we
				// can state that the barrier precedes candidate. In that case, candidate becomes the new barrier of
				// currentToken, and replica start of the latter, being it the direct successor of a barrier, would
				// be node next.
				barrier = candidate
			} else {
				continue
			}
		} else if currZone == next.getOwningInstance().zone && currToken.getReplicatedOwnership() > t.calculateDistanceAsFloat64(candidate, currToken) {
			// If the barrier of the current token
			// Does this really make sense?
			barrier = next
		}

		oldOwnership = currToken.getReplicatedOwnership()
		newOwnership = t.calculateDistanceAsFloat64(barrier, currToken)
		stdev = calculateImprovement(optimalTokenOwnership, newOwnership, oldOwnership, currInstance.tokenCount)
		improvement += stdev
		currInstance.adjustedOwnership += newOwnership - oldOwnership
		if newOwnership != oldOwnership {
			stats.WriteString(fmt.Sprintf("\ttoken %d [%s-%s] (old %.3f -> new %.3f, impr: %.5f)\n",
				currToken.getToken(), currInstance.instanceId, currZone.zone, oldOwnership/optimalTokenOwnership, newOwnership/optimalTokenOwnership, stdev))
		}
	}

	stats.WriteString(fmt.Sprintf("Candidate token %d affects the following instances:\n", candidate.getToken()))
	// Go through all visited instances and zones, add recorder adjustedOwnership and clear visited instances and zones
	currInstance := prevInstance
	for currInstance != nil {
		newOwnership = prevInstance.adjustedOwnership
		oldOwnership = prevInstance.ownership
		tokenCount := float64(prevInstance.tokenCount)
		diff := sq(oldOwnership/tokenCount-optimalTokenOwnership) - sq(newOwnership/tokenCount-optimalTokenOwnership)
		if currInstance == newInstance {
			diff *= multiplier
		}
		improvement += diff
		prevInstance = currInstance.precededBy
		currInstance.precededBy = nil
		currInstance.zone.precededBy = nil
		currInstance.adjustedOwnership = 0.0
		if newOwnership != oldOwnership {
			oldOptimalTokenOwnership := tokenCount * optimalTokenOwnership
			newOptimalTokenOwnership := oldOptimalTokenOwnership
			if currInstance == newInstance {
				newOptimalTokenOwnership += optimalTokenOwnership
			}
			stats.WriteString(fmt.Sprintf("\t[%s-%s] (old %.3f -> new %.3f, impr: %.5f)\n",
				currInstance.instanceId, currInstance.zone.zone, oldOwnership/oldOptimalTokenOwnership, newOwnership/newOptimalTokenOwnership, diff))
		}
		currInstance = prevInstance
	}
	stats.WriteString(fmt.Sprintf("Evaluation of candidate %d [%s-%s] gives rise to the following improvement: %.5f\n",
		candidate.getToken(), newInstance.instanceId, newInstanceZone.zone, improvement))
	//fmt.Print(stats.String())

	return improvement
}

func (t *TokenDistributor) getStats(tokenInfoCircularList *CircularList[*tokenInfo], optimalTokenOwnership float64) [2][4]float64 {
	result := [2][4]float64{}
	head := tokenInfoCircularList.head
	curr := head
	result[0][0] = math.MaxFloat64
	result[0][1] = math.SmallestNonzeroFloat64
	result[0][2] = 0.0
	result[0][3] = 0.0
	result[1][0] = math.MaxFloat64
	result[1][1] = math.SmallestNonzeroFloat64
	result[1][2] = 0.0
	result[1][3] = 0.0
	for {
		dist := curr.getData().getReplicatedOwnership() / optimalTokenOwnership
		if result[0][0] > dist {
			result[0][0] = dist
		}
		if result[0][1] < dist {
			result[0][1] = dist
		}
		result[0][2] = result[0][2] + sq(dist-1.0)
		result[0][3] = result[0][3] + curr.getData().getReplicatedOwnership()
		dist = curr.getData().getOwningInstance().ownership / (optimalTokenOwnership * float64(t.tokensPerInstance))
		if result[1][0] > dist {
			result[1][0] = dist
		}
		if result[1][1] < dist {
			result[1][1] = dist
		}
		result[1][2] = result[1][2] + sq(dist-1.0)
		result[1][3] = result[1][3] + curr.getData().getOwningInstance().ownership
		curr = curr.next
		if curr == head {
			break
		}
	}
	head = tokenInfoCircularList.head
	curr = head
	termInstance := newInstanceInfo("term", nil, 0)
	prevInstance := termInstance
	for {
		instance := curr.getData().getOwningInstance()
		if instance.precededBy == nil {
			fmt.Printf("Instance [%s-%s-%.2f]\n", curr.getData().getOwningInstance().instanceId, curr.getData().getOwningInstance().zone.zone, curr.getData().getOwningInstance().ownership)
			instance.precededBy = prevInstance
			prevInstance = instance
		} else {
			fmt.Printf("\t Already seen Instance [%s-%s-%.2f]\n", curr.getData().getOwningInstance().instanceId, curr.getData().getOwningInstance().zone.zone, curr.getData().getOwningInstance().ownership)
		}

		curr = curr.next
		if curr == head {
			break
		}
	}
	currInstance := prevInstance
	for currInstance != termInstance {
		prevInstance = currInstance.precededBy
		currInstance.precededBy = nil
		currInstance = prevInstance
	}

	return result
}

func sq(value float64) float64 {
	return value * value
}

func calculateImprovement(optimalTokenOwnership, newOwnership, oldOwnership float64, tokenCount int) float64 {
	return (sq(oldOwnership-optimalTokenOwnership) - sq(newOwnership-optimalTokenOwnership)) / sq(float64(tokenCount))
}

func (t *TokenDistributor) createPriorityQueue(candidateTokenInfoCircularList *CircularList[*candidateTokenInfo], optimalTokenOwnership float64, tokenCount int) *PriorityQueue[*candidateTokenInfo] {
	pq := newPriorityQueue[*candidateTokenInfo](len(t.sortedTokens), true)
	head := candidateTokenInfoCircularList.head
	curr := head
	for {
		improvement := t.evaluateImprovement(curr.getData(), optimalTokenOwnership, 1/float64(tokenCount))
		weightedNavigableToken := newWeightedNavigableToken[*candidateTokenInfo](improvement)
		setNavigableToken(weightedNavigableToken, curr)
		pq.Add(weightedNavigableToken)
		curr = curr.next
		if curr == head {
			break
		}
	}
	heap.Init(pq)
	return pq
}

func (t *TokenDistributor) addCandidateToTokenInfoCircularList(candidate *candidateTokenInfo) {
	host := candidate.host
	next := host.getNext()
	newInstance := candidate.getOwningInstance()
	newInstanceZone := newInstance.zone
	old := newInstance.ownership
	newTokenInfo := newTokenInfo(newInstance, candidate.getToken())
	newTokenInfo.setReplicaStart(candidate.getReplicaStart())
	newTokenInfo.setReplicatedOwnership(t.calculateReplicatedOwnership(newTokenInfo, newTokenInfo.getReplicaStart().(navigableTokenInterface)))
	navigableToken := newNavigableTokenInfo(newTokenInfo)
	navigableToken.insertBefore(next.getNavigableToken())

	visitedZonesCount := 0
	prevZone := &LastZoneInfo
	for currToken := next; visitedZonesCount < t.replicationStrategy.getReplicationFactor(); currToken = currToken.getNext() {
		currInstance := currToken.getOwningInstance()
		currZone := currInstance.zone
		// if this zone has already been visited, we go on
		if currZone.precededBy != nil {
			continue
		}
		// otherwise we mark this zone as visited and increase the counter of visited zones
		currZone.precededBy = prevZone
		prevZone = currZone
		visitedZonesCount++

		//newReplicaStart := currToken.getReplicaStart()
		barrier := currToken.getReplicaStart().(navigableTokenInterface).getPrevious()
		if currToken.isExpandable() {
			if currToken.getReplicaStart() == next {
				// If currToken is expandable (wrt. candidate's zone), then it is possible to extend its replica set by adding
				// a token from  candidate's zone as a direct predecessor of currToken replica start. Token candidate is being
				// evaluated as the direct predecessor of token next, and therefore only that case would be valid here. In this
				// case candidate would become the new replica start of currToken.
				currToken.setReplicaStart(newTokenInfo)
				// since the new replica set is candidate, it cannot be extendable by another token from its zone
				currToken.setExpandable(false)
				barrier = host
			} else {
				continue
			}
		} else if currZone == newInstanceZone {
			if currToken.getReplicatedOwnership() > t.calculateDistanceAsFloat64(candidate, currToken) {
				// Barrier of a token is the first token belonging to the same zone of the former by crossing the ring
				// backwards, i.e., it is the direct predecessor of replica start of that token.
				// If currToken and candidate belong to the same zone, the only change that could happen here is if
				// candidate becomes a new barrier of currToken, and this is possible only if the current barrier of
				// currentToken precedes candidate.
				// Proof: if the current barrier of currentToken succeeds candidate, candidate would be at least the
				// second occurrence of the zone of currentToken, which contradicts the definition of term barrier.
				// In order to check whether the current barrier precedes candidate, we could compare the current
				// replicated weight of currToken (i.e., the range of tokens from its barrier to curentToken itself)
				// with the range of tokens from candidate to currentToken itself, and if the former is greater, we
				// can state that the barrier precedes candidate. In that case, candidate becomes the new barrier of
				// currentToken, and replica start of the latter, being it the direct successor of a barrier, would
				// be node next.
				//newReplicaStart = next
				currToken.setReplicaStart(next)
				// currToken and candidate belong to the same zone, and candidate is a new barrier of currToken. Hence,
				// currToken cannot be expandable.
				currToken.setExpandable(false)
				barrier = candidate
			} else {
				continue
			}
		}
		oldOwnership := currToken.getReplicatedOwnership()
		newOwnership := t.calculateDistanceAsFloat64(barrier, currToken)
		currToken.setReplicatedOwnership(newOwnership)
		currInstance.ownership += newOwnership - oldOwnership
	}

	currZone := prevZone
	for currZone != &LastZoneInfo {
		prevZone = currZone.precededBy
		currZone.precededBy = nil
		currZone = prevZone
	}
	fmt.Printf("Token %d has been added to the list, instance %s %.3f->%.3f\n", candidate.getToken(), newInstance, old, newInstance.ownership)
}

func (t *TokenDistributor) addNewInstanceAndToken(instance *instanceInfo, token Token) {
	t.sortedTokens = append(t.sortedTokens, token)
	slices.Sort(t.sortedTokens)
	t.instanceByToken[token] = instance.instanceId
	t.addTokenToInstance(instance.instanceId, token)
}

func (t *TokenDistributor) addFirstInstanceOfZone(instance Instance, zone Zone) {
	for _, token := range t.seedByZone[zone] {
		t.sortedTokens = append(t.sortedTokens, token)
		t.addTokenToInstance(instance, token)
		t.instanceByToken[token] = instance
	}
	t.seedByZone[zone] = nil
	slices.Sort(t.sortedTokens)
}

func (t *TokenDistributor) AddInstance(instance Instance, zone Zone) (*CircularList[*tokenInfo], *CircularList[*candidateTokenInfo]) {
	t.replicationStrategy.addInstance(instance, zone)
	if t.seedByZone[zone] != nil {
		t.addFirstInstanceOfZone(instance, zone)
		t.seedByZone[zone] = nil
		return nil, nil
	}

	instanceInfoByInstance, zoneInfoByZone := t.createInstanceAndZoneInfos()
	newInstanceZone, ok := zoneInfoByZone[zone]
	if !ok {
		newInstanceZone = newZoneInfo(zone)
		zoneInfoByZone[zone] = newInstanceZone
	}
	tokenInfoCircularList := t.createTokenInfoCircularList(instanceInfoByInstance, newInstanceZone)
	optimalTokenOwnership := t.getOptimalTokenOwnership()
	initStats := t.getStats(tokenInfoCircularList, optimalTokenOwnership)
	newInstanceInfo := newInstanceInfo(instance, newInstanceZone, t.tokensPerInstance)
	newInstanceInfo.ownership = float64(t.tokensPerInstance) * optimalTokenOwnership
	candidateTokenInfoCircularList := t.createCandidateTokenInfoCircularList(tokenInfoCircularList, newInstanceInfo, optimalTokenOwnership)

	pq := t.createPriorityQueue(candidateTokenInfoCircularList, optimalTokenOwnership, t.tokensPerInstance)
	//fmt.Printf("Priority queue: %s\n", pq)
	bestToken := heap.Pop(pq).(*WeightedNavigableToken[*candidateTokenInfo])
	//fmt.Printf("Token %s [%.5f] is the best candidate\n", bestToken.navigableToken, bestToken.weight)
	candidateTokenInfoCircularList.remove(bestToken.navigableToken)

	addedTokens := 0
	for {
		t.addCandidateToTokenInfoCircularList(bestToken.navigableToken.getData())
		t.addNewInstanceAndToken(newInstanceInfo, bestToken.navigableToken.getData().getToken())
		addedTokens++
		if addedTokens == t.tokensPerInstance {
			break
		}

		for {
			//fmt.Printf("Iteration %d, priority queue: %s\n", i, pq)
			bestToken = heap.Pop(pq).(*WeightedNavigableToken[*candidateTokenInfo])
			//fmt.Printf("\ttokenInfoCircularList: %s\n", tokenInfoCircularList)
			candidate := getNavigableToken[*candidateTokenInfo](bestToken).getData()
			// at this point i new tokens have already been added to the new instance,
			// so we are looking for the (i + 1)st candidate. Therefore, the multiplier
			// tho use in eveluateImprovement is (i + 1) / t.tokensPerInstance
			newImprovement := t.evaluateImprovement(candidate, optimalTokenOwnership, float64(addedTokens+1)/float64(t.tokensPerInstance))
			nextBestToken := pq.Peek()
			//fmt.Printf("Understanding whether token %s [%.5f] is still the best. Its new weight is %.5f, and the current elemement with the max weight in pq is %s [%.5f]\n", bestToken.navigableToken, bestToken.weight, newImprovement, nextBestToken.navigableToken, nextBestToken.weight)
			if newImprovement >= nextBestToken.weight {
				//fmt.Printf("Token %s [%.5f] is the best candidate\n", bestToken.navigableToken, bestToken.weight)
				candidateTokenInfoCircularList.remove(bestToken.navigableToken)
				break
			}
			navigableToken := getNavigableToken[*candidateTokenInfo](bestToken)
			bestToken = newWeightedNavigableToken[*candidateTokenInfo](newImprovement)
			setNavigableToken[*candidateTokenInfo](bestToken, navigableToken)
			heap.Push(pq, bestToken)
			//fmt.Printf("Token %s was not the best, so we put it back on pq and retry\n", bestToken.navigableToken)
		}
	}

	finalStats := t.getStats(tokenInfoCircularList, optimalTokenOwnership)
	fmt.Printf("Final distribution: %s\n", tokenInfoCircularList.StringVerobose())
	fmt.Printf("Token - old min ownership: %.2f%%, old max ownership: %.2f%%, old stdev: %.2f, old sum: %.2f\n", initStats[0][0], initStats[0][1], initStats[0][2], initStats[0][3])
	fmt.Printf("Instance - old min ownership: %.2f%%, old max ownership: %.2f%%, old stdev: %.2f, old sum: %.2f\n", initStats[1][0], initStats[1][1], initStats[1][2], initStats[1][3])
	fmt.Printf("Token - new min ownership: %.2f%%, new max ownership: %.2f%%, new stdev: %.2f, new sum: %.2f\n", finalStats[0][0], finalStats[0][1], finalStats[0][2], finalStats[0][3])
	fmt.Printf("Instance - new min ownership: %.2f%%, new max ownership: %.2f%%, new stdev: %.2f, new sum: %.2f\n", finalStats[1][0], finalStats[1][1], finalStats[1][2], finalStats[1][3])
	fmt.Println(instanceInfoByInstance)
	return tokenInfoCircularList, candidateTokenInfoCircularList
}
