package eventokendistributor

import (
	"container/heap"
	"fmt"
	"math/bits"
	"sort"
	"time"

	"github.com/grafana/dskit/ring"
	"golang.org/x/exp/slices"
)

var (
	errorZoneSetTooBig = func(zonesCount, tokensPerInstance int) error {
		return fmt.Errorf("number of zones %d is too big: with that number of zones it is impossible to place %d tokens per instance", zonesCount, tokensPerInstance)
	}
	errorTokensShouldBeEmpty = func(zone string, tokensCount int) error {
		return fmt.Errorf("there should be no tokens for zone %s, but actually %d tokes are already present", zone, tokensCount)
	}
	errorNotAllTokenCreated = func(instanceID int, zone string, tokensPerInstance int) error {
		return fmt.Errorf("impossible to find all %d tokens: it was not possible to create all required tokens for instance %d in zone %s", tokensPerInstance, instanceID, zone)
	}
)

type SpreadMinimizingRing struct {
	Ring
}

func NewSpreadMinimizingRing(ringDesc *ring.Desc, zones []string, tokensPerInstance int, maxTokenValue uint32) *SpreadMinimizingRing {
	sort.Strings(zones)
	return &SpreadMinimizingRing{
		Ring{
			ringDesc:          ringDesc,
			zones:             zones,
			tokensPerInstance: tokensPerInstance,
			maxTokenValue:     maxTokenValue,
		},
	}
}

// AddInstance adds the instance with the given id from the given zone to the ring. It also assigns the set of unique,
// sorted and equidistant tokens to the new instance.
func (r *SpreadMinimizingRing) AddInstance(instanceID int, zone string) error {
	instancesByZone := r.getInstancesByZone()
	instances, ok := instancesByZone[zone]
	if !ok {
		instances = map[string]ring.InstanceDesc{}
	}
	instance := fmt.Sprintf("instance-%s-%d", zone, instanceID)
	_, ok = instances[instance]
	if ok {
		return fmt.Errorf("instance %s with id %d already existis in zone %s", instance, instanceID, zone)
	}
	zoneID, err := r.getZoneID(zone)
	if err != nil {
		return err
	}
	tokens, err := r.CalculateTokensEvenly(instanceID, zoneID)
	if err != nil {
		return err
	}

	r.ringDesc.AddIngester(instance, instance, zone, tokens, ring.ACTIVE, time.Now())
	return nil
}

func (r *SpreadMinimizingRing) CalculateTokens(instanceID, zoneID int) (ring.Tokens, error) {
	zone := r.zones[zoneID]
	ownershipByInstanceByZone, ownershipByTokenByZone := r.getRegisteredOwnershipByZone()
	ownershipByInstance := ownershipByInstanceByZone[zone]
	ownershipByToken := ownershipByTokenByZone[zone]
	instanceByToken := r.GetInstanceByToken()
	instancesCount := len(ownershipByInstance)
	if instancesCount == 0 {
		return r.calculateInitialTokens(zoneID, zone)
	}
	queues := r.createTokenOwnershipPriorityQueues(instancesCount, ownershipByToken, instanceByToken)
	optimalInstanceOwnership := (float64(r.maxTokenValue) + 1) / float64(instancesCount+1)
	optimalTokenOwnership := optimalInstanceOwnership / float64(r.tokensPerInstance)
	optimalTokensPerInstance := r.tokensPerInstance / instancesCount
	tokens := make(ring.Tokens, 0, r.tokensPerInstance)
	addedTokens := 0
	newInstanceOwnership := 0.0
	for instance := range ownershipByInstance {
		queue := queues[instance]
		for t := 0; t < optimalTokensPerInstance; t++ {
			ownershipInfo := queue.Peek()
			if ownershipInfo.ownership > optimalTokenOwnership {
				heap.Pop(queue)
				ringToken := ownershipInfo.ringItem.(*RingToken)
				token := r.calculateNewToken(ringToken.token, ownershipInfo.ownership, uint32(optimalTokenOwnership), uint32(len(r.zones)), uint32(zoneID))
				tokens = append(tokens, token)
				oldOwnership := ownershipInfo.ownership
				ownershipInfo.ownership = float64(distance(token, ringToken.token, r.maxTokenValue))
				newInstanceOwnership += oldOwnership - ownershipInfo.ownership
				heap.Push(queue, ownershipInfo)
				ownershipByInstance[instance] = ownershipByInstance[instance] - oldOwnership + ownershipInfo.ownership
				addedTokens++
			}
		}
	}
	if addedTokens != r.tokensPerInstance {
		instanceQueue := r.createInstanceOwnershipPriorityQueue(ownershipByInstance)
		for t := addedTokens; t < r.tokensPerInstance; t++ {
			optimalTokenOwnership = (optimalInstanceOwnership - newInstanceOwnership) / float64(r.tokensPerInstance-t)
			instanceOwnershipInfo := instanceQueue.Peek()
			if instanceOwnershipInfo.ownership < optimalTokenOwnership {
				fmt.Printf("It was impossible to add %dth token because instance with the highest ownership %15.3f cannot satify the request %15.3f\n", t+1, instanceOwnershipInfo.ownership, optimalTokenOwnership)
				break
			}
			ringInstance := instanceOwnershipInfo.ringItem.(*RingInstance)
			heap.Pop(instanceQueue)
			queue := queues[ringInstance.instance]
			tokenOwnershipInfo := queue.Peek()
			if tokenOwnershipInfo.ownership < optimalTokenOwnership {
				continue
			}
			heap.Pop(queue)
			ringToken := tokenOwnershipInfo.ringItem.(*RingToken)
			token := r.calculateNewToken(ringToken.token, tokenOwnershipInfo.ownership, uint32(optimalTokenOwnership), uint32(len(r.zones)), uint32(zoneID))
			tokens = append(tokens, token)
			oldOwnership := tokenOwnershipInfo.ownership
			tokenOwnershipInfo.ownership = float64(distance(token, ringToken.token, r.maxTokenValue))
			newInstanceOwnership += oldOwnership - tokenOwnershipInfo.ownership
			heap.Push(queue, tokenOwnershipInfo)
			ownershipByInstance[ringInstance.instance] = ownershipByInstance[ringInstance.instance] - oldOwnership + tokenOwnershipInfo.ownership
			instanceOwnershipInfo.ownership = instanceOwnershipInfo.ownership - oldOwnership + tokenOwnershipInfo.ownership
			heap.Push(instanceQueue, instanceOwnershipInfo)
			addedTokens++
		}
	}
	existingTokens := r.GetTokens()
	for _, token := range tokens {
		if slices.Contains(existingTokens, token) {
			return nil, errorTokenAlreadyTaken(token, instanceID, zoneID)
		}
	}
	slices.Sort(tokens)
	return tokens, nil
}

func (r *SpreadMinimizingRing) CalculateTokensEvenly(instanceID, zoneID int) (ring.Tokens, error) {
	zone := r.zones[zoneID]
	ownershipByInstanceByZone, ownershipByTokenByZone := r.getRegisteredOwnershipByZone()
	ownershipByInstance := ownershipByInstanceByZone[zone]
	ownershipByToken := ownershipByTokenByZone[zone]
	instanceByToken := r.GetInstanceByToken()
	instancesCount := len(ownershipByInstance)
	if instancesCount == 0 {
		return r.calculateInitialTokens(zoneID, zone)
	}
	queues := r.createTokenOwnershipPriorityQueues(instancesCount, ownershipByToken, instanceByToken)
	optimalInstanceOwnership := (float64(r.maxTokenValue) + 1) / float64(instancesCount+1)
	optimalTokenOwnership := optimalInstanceOwnership / float64(r.tokensPerInstance)
	tokens := make(ring.Tokens, 0, r.tokensPerInstance)
	addedTokens := 0
	newInstanceOwnership := 0.0
	instanceQueue := r.createInstanceOwnershipPriorityQueue(ownershipByInstance)
	for t := addedTokens; t < r.tokensPerInstance; t++ {
		optimalTokenOwnership = (optimalInstanceOwnership - newInstanceOwnership) / float64(r.tokensPerInstance-t)
		instanceOwnershipInfo := instanceQueue.Peek()
		if instanceOwnershipInfo.ownership < optimalTokenOwnership {
			fmt.Printf("It was impossible to add %dth token because instance with the highest ownership %15.3f cannot satify the request %15.3f\n", t+1, instanceOwnershipInfo.ownership, optimalTokenOwnership)
			break
		}
		ringInstance := instanceOwnershipInfo.ringItem.(*RingInstance)
		heap.Pop(instanceQueue)
		queue := queues[ringInstance.instance]
		tokenOwnershipInfo := queue.Peek()
		if tokenOwnershipInfo.ownership < optimalTokenOwnership {
			continue
		}
		heap.Pop(queue)
		ringToken := tokenOwnershipInfo.ringItem.(*RingToken)
		token := r.calculateNewToken(ringToken.token, tokenOwnershipInfo.ownership, uint32(optimalTokenOwnership), uint32(len(r.zones)), uint32(zoneID))
		tokens = append(tokens, token)
		oldOwnership := tokenOwnershipInfo.ownership
		tokenOwnershipInfo.ownership = float64(distance(token, ringToken.token, r.maxTokenValue))
		newInstanceOwnership += oldOwnership - tokenOwnershipInfo.ownership
		heap.Push(queue, tokenOwnershipInfo)
		ownershipByInstance[ringInstance.instance] = ownershipByInstance[ringInstance.instance] - oldOwnership + tokenOwnershipInfo.ownership
		instanceOwnershipInfo.ownership = instanceOwnershipInfo.ownership - oldOwnership + tokenOwnershipInfo.ownership
		heap.Push(instanceQueue, instanceOwnershipInfo)
		addedTokens++
	}
	if addedTokens != r.tokensPerInstance {
		return nil, errorNotAllTokenCreated(instanceID, zone, r.tokensPerInstance)
	}
	existingTokens := r.GetTokens()
	for _, token := range tokens {
		if slices.Contains(existingTokens, token) {
			return nil, errorTokenAlreadyTaken(token, instanceID, zoneID)
		}
	}
	slices.Sort(tokens)
	return tokens, nil
}

func (r *SpreadMinimizingRing) calculateNewToken(token uint32, ownership float64, optimalTokenOwnership, zoneLen, zoneOffset uint32) uint32 {
	offset := uint32(ownership) - optimalTokenOwnership
	if offset >= token {
		newToken := r.maxTokenValue - offset + token
		newToken = (newToken/zoneLen)*zoneLen - zoneLen + zoneOffset
		return newToken
	}
	newToken := token - offset
	newToken = (newToken/zoneLen)*zoneLen + zoneOffset
	return newToken
}

// CalculateInitialTokens calculates a set of tokens for a given zone that will be assigned
// to the first instance of that zone.
func (r *SpreadMinimizingRing) calculateInitialTokens(zoneID int, zone string) (ring.Tokens, error) {
	existingTokens := r.getTokensByZone()
	existingTokensInZone, ok := existingTokens[zone]
	if ok {
		return nil, errorTokensShouldBeEmpty(zone, len(existingTokensInZone))
	}
	tokenDistance := (float64(r.maxTokenValue) + 1) / float64(r.tokensPerInstance)
	tokenDistanceLen := bits.Len(uint(tokenDistance) - 1)
	zonesOffsetLen := bits.Len(uint(len(r.zones) - 1))
	if zonesOffsetLen >= tokenDistanceLen {
		return nil, errorZoneSetTooBig(len(r.zones), r.tokensPerInstance)
	}
	//fmt.Printf("id: %3d, binary offset: %032b, numeric offset: %d\n", instanceID, offset, offset)
	tokens := make(ring.Tokens, 0, r.tokensPerInstance)
	zoneLen := uint32(len(r.zones))
	for i := 0; i < r.tokensPerInstance; i++ {
		token := uint32(i << tokenDistanceLen)
		token = (token/zoneLen)*zoneLen + uint32(zoneID)
		tokens = append(tokens, token)
	}
	return tokens, nil
}

func (r *SpreadMinimizingRing) createTokenOwnershipPriorityQueues(instancesCount int, ownershipByToken map[uint32]float64, instanceByToken map[uint32]InstanceInfo) map[string]*PriorityQueue {
	queues := make(map[string]*PriorityQueue, instancesCount)
	for token, ownership := range ownershipByToken {
		instance := instanceByToken[token]
		queue, ok := queues[instance.InstanceID]
		if !ok {
			queue = newPriorityQueue(r.tokensPerInstance, true)
			queues[instance.InstanceID] = queue
		}
		ownershipInfo := newOwnershipInfo(ownership)
		ownershipInfo.SetRingItem(NewRingToken(token, token))
		queue.Add(ownershipInfo)
	}
	for _, queue := range queues {
		heap.Init(queue)
	}
	return queues
}

func (r *SpreadMinimizingRing) createInstanceOwnershipPriorityQueue(ownershipByInstance map[string]float64) *PriorityQueue {
	queue := newPriorityQueue(len(ownershipByInstance), true)
	for instance, ownership := range ownershipByInstance {
		ownershipInfo := newOwnershipInfo(ownership)
		ownershipInfo.SetRingItem(NewRingInstance(0, instance))
		queue.Add(ownershipInfo)
	}
	heap.Init(queue)
	return queue
}
