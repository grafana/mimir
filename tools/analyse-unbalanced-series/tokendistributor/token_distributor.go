package tokendistributor

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
}

func newTokenDistributor(tokensPerInstance int, replicationStrategy ReplicationStrategy) *TokenDistributor {
	sortedTokens := make([]Token, 0, initialInstanceCount*tokensPerInstance)
	instanceByToken := make(map[Token]Instance, initialInstanceCount*tokensPerInstance)
	tokensByInstance := make(map[Instance][]Token, initialInstanceCount)

	return &TokenDistributor{
		sortedTokens:        sortedTokens,
		instanceByToken:     instanceByToken,
		replicationStrategy: replicationStrategy,
		tokensByInstance:    tokensByInstance,
		maxTokenValue:       maxTokenValue,
	}
}

// newTokenDistributorFromInitializedInstances supposes that the ring already contains some instances with tokensPerInstance tokens each
func newTokenDistributorFromInitializedInstances(sortedTokens []Token, instanceByToken map[Token]Instance, tokensPerInstance int, replicationStrategy ReplicationStrategy) *TokenDistributor {
	tokensByInstance := make(map[Instance][]Token, len(sortedTokens)/tokensPerInstance)
	for token, instance := range instanceByToken {
		tokens, ok := tokensByInstance[instance]
		if !ok {
			tokens = make([]Token, 0, tokensPerInstance)
		}
		tokens = append(tokens, token)
		tokensByInstance[instance] = tokens
	}
	return &TokenDistributor{
		sortedTokens:        sortedTokens,
		instanceByToken:     instanceByToken,
		replicationStrategy: replicationStrategy,
		tokensPerInstance:   tokensPerInstance,
		tokensByInstance:    tokensByInstance,
		maxTokenValue:       maxTokenValue,
	}
}

func (t TokenDistributor) getInstanceCount() int {
	return len(t.tokensByInstance)
}

func (t TokenDistributor) getOptimalTokenOwnership(newTokensCount int) float64 {
	maxTokenAsFloat64 := float64(t.maxTokenValue)
	return maxTokenAsFloat64 * float64(t.replicationStrategy.getReplicationFactor()) / float64(len(t.sortedTokens)+newTokensCount)
}

func (t TokenDistributor) createInstanceAndZoneInfos() (map[Instance]*instanceInfo, map[Zone]*zoneInfo) {
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
func (t TokenDistributor) createTokenInfoCircularList(instanceInfoByInstance map[Instance]*instanceInfo, newInstanceZone *zoneInfo) *CircularList[*tokenInfo] {
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
func (t TokenDistributor) createCandidateTokenInfoCircularList(tokenInfoCircularList *CircularList[*tokenInfo], newInstanceInfo *instanceInfo, initialTokenOwnership float64) *CircularList[*candidateTokenInfo] {
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

func (t TokenDistributor) calculateCandidateToken(tokenInfo *tokenInfo) Token {
	currentToken := tokenInfo.getToken()
	nextToken := tokenInfo.getNavigableToken().getNext().getToken()
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
func (t TokenDistributor) calculateReplicaStartAndExpansion(navigableToken navigableTokenInterface, newInstanceZone *zoneInfo) (navigableTokenInterface, bool) {
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
		curr = curr.getNavigableToken().getPrev()
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
// in the ring. Moreover, it updates the replicated ownership information of both token itself and the instance that
// corresponds to that node.
func (t TokenDistributor) populateTokenInfo(tokenInfo *tokenInfo, newInstanceZone *zoneInfo) {
	replicaStart, expandable := t.calculateReplicaStartAndExpansion(tokenInfo, newInstanceZone)
	tokenInfo.setReplicaStart(replicaStart.getToken())
	tokenInfo.setExpandable(expandable)
	newOwnership := float64(replicaStart.getNavigableToken().getPrev().getToken().distance(tokenInfo.getToken(), t.maxTokenValue))
	oldOwnership := tokenInfo.getReplicatedOwnership()
	tokenInfo.setReplicatedOwnership(newOwnership)
	tokenInfo.getOwningInstance().ownership += newOwnership - oldOwnership
}

// populateCandidateTokenInfo gets as input a candidateTokenInfo, it calculates and sets its replica start and expandable
// information starting from the position in the ring that corresponds to candidateTokenInfo's host, i.e., the token after
// which the candidate is being added to the ring
func (t TokenDistributor) populateCandidateTokenInfo(candidateTokenInfo *candidateTokenInfo, newInstanceZone *zoneInfo) {
	replicaStart, expandable := t.calculateReplicaStartAndExpansion(candidateTokenInfo, newInstanceZone)
	candidateTokenInfo.setReplicaStart(replicaStart.getToken())
	candidateTokenInfo.setExpandable(expandable)
}
