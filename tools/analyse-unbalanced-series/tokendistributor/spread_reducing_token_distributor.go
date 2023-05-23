package tokendistributor

import "C"
import (
	"container/heap"
	"fmt"
	"math"
	"strings"

	"golang.org/x/exp/slices"
)

type SpreadReducingTokenDistributor struct {
	*TokenDistributor
}

func NewSpreadReducingTokenDistributor(tokensPerInstance, zonesCount int, maxTokenValue Token, replicationStrategy ReplicationStrategy, seedGenerator SeedGenerator) *SpreadReducingTokenDistributor {
	return &SpreadReducingTokenDistributor{
		NewTokenDistributor(tokensPerInstance, zonesCount, maxTokenValue, replicationStrategy, seedGenerator),
	}
}

func (t *SpreadReducingTokenDistributor) AddInstance(instance Instance, zone Zone) (*CircularList[*tokenInfo], *CircularList[*candidateTokenInfo], *OwnershipInfo, error) {
	t.replicationStrategy.addInstance(instance, zone)
	if t.seedGenerator != nil && t.seedGenerator.hasNextSeed(zone) {
		t.addTokensFromSeed(instance, zone)
		return nil, nil, &OwnershipInfo{}, nil
	}

	instanceInfoByInstance, _ := t.createInstanceAndZoneInfos(&zone, &instance)
	newInstanceInfo := instanceInfoByInstance[instance]
	tokenInfoCircularList := t.createTokenInfoCircularList(instanceInfoByInstance, newInstanceInfo)
	//fmt.Printf("\t\t\t%s", instanceInfoByInstance)
	optimalTokenOwnership := t.getOptimalTokenOwnership(true)
	candidateTokenInfoCircularList := t.createCandidateTokenInfoCircularList(tokenInfoCircularList, newInstanceInfo, optimalTokenOwnership)

	addedTokens := 0
	for {
		pq := t.createPriorityQueue(candidateTokenInfoCircularList)
		//fmt.Printf("Priority queue: %s\n", pq)
		bestToken := heap.Pop(pq).(*WeightedNavigableToken[*candidateTokenInfo])
		//fmt.Printf("Token %s [%.5f] is the best candidate\n", bestToken.navigableToken, bestToken.weight)
		bestCandidate := bestToken.navigableToken
		t.addNewInstanceAndToken(instance, zone, bestToken.navigableToken.getData().getToken())
		err := t.addCandidateAndUpdateTokenInfoCircularList(bestCandidate, tokenInfoCircularList)
		//fmt.Printf("Token %d added to the list\n", bestCandidate.getData().token)
		//fmt.Println(tokenInfoCircularList.StringVerobose())
		//fmt.Println("----------------------------")
		if err != nil {
			return nil, nil, nil, err
		}
		candidateTokenInfoCircularList.remove(bestToken.navigableToken)
		addedTokens++
		pq.Clear()
		if addedTokens == t.tokensPerInstance {
			break
		}
	}

	ownershipInfo := t.createOwnershipInfo(tokenInfoCircularList, optimalTokenOwnership)

	//t.count(tokenInfoCircularList)
	//fmt.Println("-----------------------------------------------------------")
	return tokenInfoCircularList, candidateTokenInfoCircularList, ownershipInfo, nil
}

func (t *SpreadReducingTokenDistributor) getMinMaxOwnership(tokenInfoCircularList *CircularList[*tokenInfo]) (float64, float64) {
	head := tokenInfoCircularList.head
	curr := head
	minOwnership := math.MaxFloat64
	maxOwnership := math.SmallestNonzeroFloat64
	for {
		minOwnership = math.Min(minOwnership, curr.getData().getReplicatedOwnership())
		maxOwnership = math.Max(maxOwnership, curr.getData().getReplicatedOwnership())
		curr = curr.next
		if curr == head {
			break
		}
	}
	return minOwnership, maxOwnership
}

func (t *SpreadReducingTokenDistributor) sortInstanceInfosByOwnership(instanceInfos []*instanceInfo) {
	slices.SortFunc(instanceInfos, func(a, b *instanceInfo) bool {
		return a.ownership < b.ownership
	})
}

func (t *SpreadReducingTokenDistributor) createPriorityQueue(candidateTokenInfoCircularList *CircularList[*candidateTokenInfo]) *PriorityQueue[*candidateTokenInfo] {
	pq := newPriorityQueue[*candidateTokenInfo](len(t.sortedTokens), false)
	head := candidateTokenInfoCircularList.head
	curr := head
	for {
		spread, err := t.evaluateSpreadAfterInsertion(curr.getData())
		if err == nil {
			weightedNavigableToken := newWeightedNavigableToken[*candidateTokenInfo](spread)
			setNavigableToken(weightedNavigableToken, curr)
			pq.Add(weightedNavigableToken)

		}
		curr = curr.next
		if curr == head {
			break
		}
	}
	heap.Init(pq)
	return pq
}

// evaluateSpreadAfterInsertion evaluates the spread that insertion of the given candidate into token list
// would bring. The spread is calculated as (maxOwnership - minOwnership) / maxOwnership.
func (t *SpreadReducingTokenDistributor) evaluateSpreadAfterInsertion(candidate *candidateTokenInfo) (float64, error) {
	candidateInstance := candidate.getOwningInstance()
	host := candidate.host
	next := host.getNext()

	// We first calculate the replicated ownership of candidate token
	newOwnership := t.calculateReplicatedOwnership(candidate, candidate.getReplicaStart().(navigableTokenInterface))
	candidateInstance.adjustedOwnership = candidateInstance.ownership + newOwnership
	stats := strings.Builder{}
	stats.WriteString(fmt.Sprintf("Adding candidate token %d (%.3f) affects the following tokens:\n", candidate.getToken(), newOwnership))

	// We now calculate potential changes in the replicated ownership of the tokenInfo that succeeds the host (i.e., next).
	// This tokenInfo (next) is of interest because candidate has been placed between host and next.

	// On the other hand. the replicated weight of next can change

	// The replicated ownership of host and tokens preceding it cannot be changed because they precede the candidate,
	// and the replicated weight is the distance between token's barrier and the token itself.
	// Therefore, we cross the ring clockwise starting from next and calculate potential improvements of all the
	// tokens that could be potentially affected by candidate's insertion. This basically means that we are looking
	// for all the tokens that replicate the candidate. We keep track of visited congruence groups, so that we don't
	// count the same group more than once. Once the count of visited groups is equal to RF, we can stop the visit.
	// The reason for that is that no other token that succeeds the token of the last visited congruence group could
	// contain a replica of candidate because between that token (included) and candidate (excluded) there would be
	// RF different congruence groups, and therefore candidate would have already been replicated RF times. In other
	// words, the replica start of that token would succeed candidate on the ring.

	newInstanceZone := candidateInstance.zone
	prevInstance := candidateInstance
	replicasCount := 0
	for currToken := next; replicasCount < t.replicationStrategy.getReplicationFactor(); currToken = currToken.getNext() {
		currInstance := currToken.getOwningInstance()
		currZone := currInstance.zone
		// if this instance has already been visited, we go on
		if t.isAlreadyVisited(currInstance) {
			continue
		}
		// otherwise we mark these zone and instance as visited
		currZone.precededBy = currZone
		replicasCount++

		// If this is the firs time we encounter an instance, we initialize its adjustedOwnership and mark it as seen.
		// The adjustedOwnership of each instance will be used for the final evaluation of that instance, together with
		// all potential improvements of all visited tokens
		if currInstance.precededBy == nil {
			currInstance.adjustedOwnership = currInstance.ownership
			currInstance.precededBy = prevInstance
			prevInstance = currInstance
		}

		barrier := currToken.getReplicaStart().(navigableTokenInterface).getPrevious()
		// We now evaluate the improvement that insertion of candidateToken into the ring would bring.
		// The only relevant parameter for calculating each token's new replicated ownership is the new
		// barrier that insertion of candidateToken would give raise to.
		// The analysis done below is based to the analysis did in addCandidateAndUpdateTokenInfoCircularList
		if currToken.isExpandable() {
			// If currToken is expandable by candidateToken, it basically means that it is possible to add
			// candidateToken before currToken's replica start without changing the number of tokens replicated
			// in currToken. The change would look like this:
			// FROM: currToken <- ... <- currToken.replicaStart <- currToken.barrier
			// TO:   currToken <- ... <- currToken.replicaStart <- candidateToken <- currToken.barrier
			// Since candidateToken is a direct successor of next, expanding currToken by candidateToken is
			// therefore possible only if currToken.replicaStart == next or, equivalently, if currentToken's
			// barrier is host.
			// currToken <- ... <- currToken.replicaStart=next <- candidateToken <- currToken.barrier=host
			// In that case, currToken gets a new replicaStart (candidateToken), it remains expandable (adding another
			// token congruent with candidateToken would not change the barrier), and its barrier remains host.
			if currToken.getReplicaStart() == next {
				barrier = host
			} else {
				continue
			}
		} else {
			// If currToken is NOT expandable, we distinguish 2 possible cases:
			// 1) whether currToken's instance is congruent with candidate's instance (always applicable) or
			// 2) otherwise, if between currToken and its barrier there are exactly RF different congruence groups,
			//    but there is no token between them that is congruent with candidates instance (applicable only if
			//    zone-awareness is disabled)
			if t.isCongruent(currInstance, candidateInstance) {
				// In this case candidateToken could be a new barrier of currToken, but only if candidateToken lays
				// between currToken and currentToken's barrier.
				// currToken <- ... <- next <- candidateToken <- ... <- currToken.barrier
				// In that case, new replica start of currToken is next, new barrier of currToken is candidateToken,
				// and currToken is not expandable by tokens congruent with candidateToken anymore: in fact, adding
				// another token congruent with candidateToken (and currentToken) in front of replica start (next),
				// that token would become a new barrier, and that would further change the replicated token ownership
				// of currToken.
				// currToken <- ... <- currToken.replicaStart = next <- currToken.barrier = candidate
				// In order to understand whether candidateToken lays between currToken and currToken's, we can compare
				// distances between currToken and its barrier (i.e., currToken.getReplicatedOwnership()) and between
				// currToken and candidateToken.
				if currToken.getReplicatedOwnership() > t.calculateDistanceAsFloat64(candidate, currToken) {
					// GOOD CASE: currToken <- ... <- next <- candidate <- ... <- currToken.barrier
					// BECOMES: currToken <- ... <- currToken.replicaStart = next <- currToken.barrier = candidate
					barrier = candidate
				} else {
					// BAD CASE: currToken <- ... <- currToken.barrier <- ... <- candidate
					continue
				}
			} else {
				// If currToken is not expandable, and it is not congruent with candidateToken, the only remaining
				// possibility is that currToken's barrier is the first occurrence of (RF)st congruence group,
				// and that there is no token congruent with candidateToken between currToken and its barrier.
				// Note: the following cases could only occur when zone-awareness is disabled.

				// There are 3 possible cases that we analyze here
				if currToken.getReplicatedOwnership() < t.calculateDistanceAsFloat64(host, currToken) {
					// Case 1: currToken's barrier lays between currToken and candidateToken
					// currToken <- ... <- currToken.barrier <- ... <- candidateToken
					// In this case, nothing can be done, i.e., insertion of candidateToken does not affect currToken
					continue
				} else if currToken.getReplicatedOwnership() == t.calculateDistanceAsFloat64(host, currToken) {
					// Case 2: currToken's barrier is host (note: host BELONGS to the ring, while candidateToken
					// DOES NOT BELONG to the ring yet, so it is possible that host is currToken's barrier).
					// In this case being host the currToken's barrier, and knowing that the barrier is the first
					// occurrence of RFst different congruence group, and that no token congruent to candidateToken
					// lays between currToken and currToken's barrier (host), candidateToken can become the new
					// barrier: in fact, it would be the first occurrence of a new congruence group. The change can
					// be depicted this way:
					// FROM: currToken <- ... <- next <- host = currToken.barrier
					// TO:   currToken <- ... <- next = currToken.replicaStart <- candidateToken = currToken.barrier <- host
					// Since candidateToken is a new barrier of currToken, the latter cannot be expanded by another
					// token from the same congruence group of candidateToken.
					barrier = candidate
				} else if currToken.getReplicatedOwnership() > t.calculateDistanceAsFloat64(host, currToken) {
					// Case 3: this is the most tricky case, and represents a situation when both candidateToken
					// (not yet in the ring) and host (already in the ring) lay between currToken and currToken's
					// barrier (but we don't know where exactly).
					// currToken <- ... <- next <- host <- ... <- currToken.barrier
					// currToken's barrier is the first occurrence of the RFs different congruence group, and there
					// is no token belonging to candidateToken's congruence group between currToken and its barrier.
					// This means that, if we add candidateToken to that path, it will be the first and only occurrence
					// of a new congruence group, and currToken's barrier must be updated. In this case it is not
					// obviously clear where, because between candidateToken and currToken's barrier there might be
					// either just repetitions of the congruence groups seen before candidateToken, or also occurrences
					// of new congruence groups. The safest way to go here is to re-apply calculateReplicaStart on
					// currToken.
					replicaStart, err := t.calculateReplicaStart(currToken.getNavigableToken().getData())
					if err != nil {
						return 0, err
					}
					barrier = replicaStart.getPrevious()
				}
			}
		}
		oldOwnership := currToken.getReplicatedOwnership()
		newOwnership = t.calculateDistanceAsFloat64(barrier, currToken)
		currInstance.adjustedOwnership += newOwnership - oldOwnership
		if newOwnership != oldOwnership {
			stats.WriteString(fmt.Sprintf("\ttoken %d [%s-%s] (old %.3f -> new %.3f)\n", currToken.getToken(), currInstance.instanceId, currZone.zone, oldOwnership, newOwnership))
		}
	}

	stats.WriteString(fmt.Sprintf("Candidate token %d affects the following instances:\n", candidate.getToken()))
	minAdjustedOwnership := candidateInstance.adjustedOwnership
	maxAdjustedOwnership := candidateInstance.adjustedOwnership
	currToken := next
	for {
		currInstance := currToken.getOwningInstance()
		ownership := currInstance.adjustedOwnership
		if currInstance.precededBy == nil {
			ownership = currInstance.ownership
		}
		minAdjustedOwnership = math.Min(minAdjustedOwnership, ownership)
		maxAdjustedOwnership = math.Max(maxAdjustedOwnership, ownership)
		currToken = currToken.getNext()
		if currToken == next {
			break
		}
	}

	// Go through all visited instances and zones, add recorder adjustedOwnership and clear visited instances and zones
	currInstance := prevInstance
	for currInstance != nil {
		oldOwnership := prevInstance.ownership
		newOwnership = prevInstance.adjustedOwnership
		prevInstance = currInstance.precededBy
		currInstance.precededBy = nil
		currInstance.zone.precededBy = nil
		currInstance.adjustedOwnership = 0.0
		if newOwnership != oldOwnership {
			stats.WriteString(fmt.Sprintf("\t[%s-%s] (old %.3f -> new %.3f)\n", currInstance.instanceId, currInstance.zone.zone, oldOwnership, newOwnership))
		}
		currInstance = prevInstance
	}
	spread := (maxAdjustedOwnership - minAdjustedOwnership) / maxAdjustedOwnership
	stats.WriteString(fmt.Sprintf("Evaluation of candidate %d [%s-%s] gives rise to the following spread: %.5f\n", candidate.getToken(), candidateInstance.instanceId, newInstanceZone.zone, spread))
	//fmt.Print(stats.String())

	return spread, nil
}
