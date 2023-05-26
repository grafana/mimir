package eventokendistributor

import (
	"container/heap"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"sort"
	"time"

	"github.com/grafana/dskit/ring"
	"golang.org/x/exp/slices"
)

var (
	errorInstanceIDTooBig = func(instanceID, zonesCount, tokensPerInstance int) error {
		return fmt.Errorf("instanceID %d is too big: with that id it is impossible to place %d tokens per instance in %d zones", instanceID, tokensPerInstance, zonesCount)
	}
	errorTokenAlreadyTaken = func(token uint32, instanceID, zoneID int) error {
		return fmt.Errorf("already existing token %d calculated for instanceID %d and zoneId %d", token, instanceID, zoneID)
	}
)

type TokenDistributorInterface interface {
	CalculateTokens(instanceID, zoneID int, existingTokens ring.Tokens) (ring.Tokens, error)
}

type Power2TokenDistributor struct {
	zones             []string
	tokensPerInstance int
	maxTokenValue     uint32
}

func NewPower2TokenDistributor(zones []string, tokensPerInstance int, maxTokenValue uint32) *Power2TokenDistributor {
	sort.Strings(zones)
	return &Power2TokenDistributor{
		zones:             zones,
		tokensPerInstance: tokensPerInstance,
		maxTokenValue:     maxTokenValue,
	}
}

/*func (t *Power2TokenDistributor) generateFirstTokens(zoneID int) ring.Tokens {
	tokens := make([]uint32, 0, t.tokensPerInstance)
	zonesCount := len(t.zones)
	for tk := 0; tk < t.tokensPerInstance; tk++ {
		token := uint32(math.Pow(2, 32)*(1.0-(3.0*float64(tk)+float64(zoneID)))/float64(zonesCount*t.tokensPerInstance)) - 1
		tokens = append(tokens, token)
	}
	slices.Sort(tokens)
	return tokens
}*/

// CalculateTokens calculates the set of tokens to be assigned to the instance with the given id from the given zone.
// id should be placed.
func (t *Power2TokenDistributor) CalculateTokens(instanceID, zoneID int, existingTokens ring.Tokens) (ring.Tokens, error) {
	tokenDistance := uint(math.MaxUint32 / t.tokensPerInstance)
	tokenDistanceLen := bits.Len(tokenDistance)
	zonesOffsetLen := bits.Len(uint((len(t.zones) - 1) << 1))
	instanceIDLen := bits.Len(uint(instanceID))
	if zonesOffsetLen+instanceIDLen >= tokenDistanceLen {
		return nil, errorInstanceIDTooBig(instanceID, len(t.zones), t.tokensPerInstance)
	}
	offset := t.calculateOffset(instanceID)
	zoneOffset := zoneID << 1
	//fmt.Printf("id: %3d, binary offset: %032b, numeric offset: %d\n", instanceID, offset, offset)
	tokens := make(ring.Tokens, 0, t.tokensPerInstance)
	for i := 0; i < t.tokensPerInstance; i++ {
		token := uint32(offset | i<<tokenDistanceLen | zoneOffset)
		tokens = append(tokens, token)
	}
	for _, token := range tokens {
		if slices.Contains(existingTokens, token) {
			return nil, errorTokenAlreadyTaken(token, instanceID, zoneID)
		}
	}
	return tokens, nil
}

// calculateOffset calculates the offset in the ring where the first token of the instance with the given
// id should be placed.
func (t *Power2TokenDistributor) calculateOffset(instanceID int) int {
	tokenRangeLen := bits.Len(uint(math.MaxUint32 / t.tokensPerInstance))
	len := bits.Len(uint(instanceID))
	if len == 0 {
		return 0
	}
	return (((instanceID - 1<<(len-1)) * 2) + 1) << (tokenRangeLen - len)
}

type SlidingPower2TokenDistributor struct {
	zones             []string
	tokensPerInstance int
	maxTokenValue     uint32
}

func NewSlidingPower2TokenDistributor(zones []string, tokensPerInstance int, maxTokenValue uint32) *SlidingPower2TokenDistributor {
	sort.Strings(zones)
	return &SlidingPower2TokenDistributor{
		zones:             zones,
		tokensPerInstance: tokensPerInstance,
		maxTokenValue:     maxTokenValue,
	}
}

// CalculateTokens calculates the set of tokens to be assigned to the instance with the given id from the given zone.
// id should be placed.
func (t *SlidingPower2TokenDistributor) CalculateTokens(instanceID, zoneID int, existingTokens ring.Tokens) (ring.Tokens, error) {
	tokenDistance := uint(math.MaxUint32 / t.tokensPerInstance)
	tokenDistanceLen := bits.Len(tokenDistance)
	zonesOffsetLen := bits.Len(uint((len(t.zones) - 1) << 1))
	instanceIDLen := bits.Len(uint(instanceID))
	if zonesOffsetLen+instanceIDLen >= tokenDistanceLen {
		return nil, errorInstanceIDTooBig(instanceID, len(t.zones), t.tokensPerInstance)
	}
	zoneOffset := zoneID << 1
	//fmt.Printf("id: %3d, binary offset: %032b, numeric offset: %d\n", instanceID, offset, offset)
	tokens := make(ring.Tokens, 0, t.tokensPerInstance)
	for tokenID := 0; tokenID < t.tokensPerInstance; tokenID++ {
		offset := t.calculateSlidingOffset(tokenID, instanceID)
		token := uint32(offset | tokenID<<tokenDistanceLen | zoneOffset)
		tokens = append(tokens, token)
	}
	for _, token := range tokens {
		if slices.Contains(existingTokens, token) {
			return nil, errorTokenAlreadyTaken(token, instanceID, zoneID)
		}
	}
	return tokens, nil
}

// calculateSlidingOffset calculates the offset in the ring where the nth token of the instance with the given
// id should be placed.
func (t *SlidingPower2TokenDistributor) calculateSlidingOffset(n, instanceID int) int {
	tokenRangeLen := bits.Len(uint(math.MaxUint32 / t.tokensPerInstance))
	len := bits.Len(uint(instanceID))
	if len == 0 {
		return 0
	}
	barrier := 1 << (len - 1)
	// slidingOffsetBase = (instanceID + n) % barrier
	slidingOffsetBase := (instanceID + n) & (barrier - 1)
	return (slidingOffsetBase*2 + 1) << (tokenRangeLen - len)
}

// distance calculates the distance between 2 tokens
func distance(first, second, maxToken uint32) uint32 {
	if second > first {
		return second - first
	}
	return maxToken - first + second
}

func tokensAsBinaryStrings(tokens ring.Tokens) []string {
	binaryStrings := make([]string, 0, len(tokens))
	for _, token := range tokens {
		binaryStrings = append(binaryStrings, fmt.Sprintf("%032b", token))
	}
	return binaryStrings
}

type RandomTokenDistributor struct {
	tokensPerInstance int
	maxTokenValue     uint32
}

func NewRandomTokenDistributor(tokensPerInstance int, maxTokenValue uint32) *RandomTokenDistributor {
	return &RandomTokenDistributor{
		tokensPerInstance: tokensPerInstance,
		maxTokenValue:     maxTokenValue,
	}
}

func (t *RandomTokenDistributor) CalculateTokens(instanceID, zoneID int, existingTokens ring.Tokens) (ring.Tokens, error) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokens := make(ring.Tokens, 0, t.tokensPerInstance)
	i := 0
	for i < t.tokensPerInstance {
		token := r.Uint32()
		if slices.Contains(existingTokens, token) {
			continue
		}
		tokens = append(tokens, token)
		i++
	}
	slices.Sort(tokens)
	return tokens, nil
}

type RandomBucketTokenDistributor struct {
	tokensPerInstance int
	maxTokenValue     uint32
}

func NewRandomBucketTokenDistributor(tokensPerInstance int, maxTokenValue uint32) *RandomBucketTokenDistributor {
	return &RandomBucketTokenDistributor{
		tokensPerInstance: tokensPerInstance,
		maxTokenValue:     maxTokenValue,
	}
}

// CalculateTokens calculates the set of tokens to be assigned to the instance with the given id from the given zone.
// id should be placed.
func (t *RandomBucketTokenDistributor) CalculateTokens(instanceID, zoneID int, existingTokens ring.Tokens) (ring.Tokens, error) {
	tokenDistance := uint(math.MaxUint32 / t.tokensPerInstance)
	tokenDistanceLen := bits.Len(tokenDistance)
	//fmt.Printf("id: %3d, binary offset: %032b, numeric offset: %d\n", instanceID, offset, offset)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	tokens := make(ring.Tokens, 0, t.tokensPerInstance)
	i := 0
	for i < t.tokensPerInstance {
		offset := r.Uint32() & (1<<tokenDistanceLen - 1)
		token := offset | uint32(i<<tokenDistanceLen)
		if slices.Contains(existingTokens, token) {
			continue
		}
		tokens = append(tokens, token)
		i++
	}
	return tokens, nil
}

type RandomModuloTokenDistributor struct {
	zones             []string
	tokensPerInstance int
	maxTokenValue     uint32
}

func NewRandomModuloTokenDistributor(zones []string, tokensPerInstance int, maxTokenValue uint32) *RandomModuloTokenDistributor {
	return &RandomModuloTokenDistributor{
		zones:             zones,
		tokensPerInstance: tokensPerInstance,
		maxTokenValue:     maxTokenValue,
	}
}

// CalculateTokens calculates the set of tokens to be assigned to the instance with the given id from the given zone.
// id should be placed.
func (t *RandomModuloTokenDistributor) CalculateTokens(instanceID, zoneID int, existingTokens ring.Tokens) (ring.Tokens, error) {
	/*pq := t.createPriorityQueue(existingTokens)
	i := 0
	for i < t.tokensPerInstance {

		i++
	}*/

	tokenDistance := uint(math.MaxUint32 / t.tokensPerInstance)
	tokenDistanceLen := bits.Len(tokenDistance)
	//fmt.Printf("id: %3d, binary offset: %032b, numeric offset: %d\n", instanceID, offset, offset)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	size := uint32(len(t.zones))
	tokens := make(ring.Tokens, 0, t.tokensPerInstance)
	i := 0
	for i < t.tokensPerInstance {
		offset := r.Uint32() & (1<<tokenDistanceLen - 1)
		token := offset | uint32(i<<tokenDistanceLen)
		token = (token/size)*size + uint32(zoneID)
		if slices.Contains(existingTokens, token) {
			continue
		}
		tokens = append(tokens, token)
		i++
	}
	return tokens, nil
}

func (t *RandomModuloTokenDistributor) createPriorityQueue(existingTokens ring.Tokens) *PriorityQueue {
	pq := newPriorityQueue(len(existingTokens), true)
	for i, token := range existingTokens {
		prevIndex := i - 1
		if prevIndex < 0 {
			prevIndex = len(existingTokens) - 1
		}
		dist := distance(existingTokens[prevIndex], token, t.maxTokenValue)
		wt := newWeightedToken(token, dist)
		pq.Add(wt)
	}
	heap.Init(pq)
	return pq
}

func (t *RandomModuloTokenDistributor) findToken(weightedToken *WeightedToken, existingTokens ring.Tokens, zoneID int) uint32 {
	half := weightedToken.weight / 2
	var candidate uint32
	if weightedToken.token < half {
		candidate = t.maxTokenValue - (half - weightedToken.token)
	} else {
		candidate = weightedToken.token - half
	}
	size := uint32(len(t.zones))
	candidate = (candidate / size) * size
	candidate += uint32(zoneID)
	for {
		if slices.Contains(existingTokens, candidate) {
			fmt.Printf("Candidate token %d has already beeen chosen. Trying another one...\n", candidate)
			candidate -= size
		} else {
			return candidate
		}
	}
}
