package eventokendistributor

import (
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
	zones             []string
	tokensPerInstance int
	maxTokenValue     uint32
}

func NewRandomTokenDistributor(zones []string, tokensPerInstance int, maxTokenValue uint32) *RandomTokenDistributor {
	sort.Strings(zones)
	return &RandomTokenDistributor{
		zones:             zones,
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
