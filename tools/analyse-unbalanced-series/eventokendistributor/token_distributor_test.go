package eventokendistributor

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

/*func TestPower2TokenDistributor_GenerateFirstTokens(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	zonesCount := len(zones)
	tokensPerInstance := 4
	td := NewPower2TokenDistributor(nil, zones, tokensPerInstance, math.MaxUint32)
	for z := 0; z < zonesCount; z++ {
		tokens := td.generateFirstTokens(z)
		require.True(t, slices.IsSorted(tokens))
		fmt.Println(tokens)
		len := tokens.Len()
		sum := uint32(0)
		prev := len - 1
		for i := 0; i < len; i++ {
			sum += distance(tokens[prev], tokens[i], math.MaxUint32)
			prev = i
		}
		require.Equal(t, uint32(math.MaxUint32), sum)
	}
}*/

func TestPower2TokenDistributor_CalculateOffset(t *testing.T) {
	tests := map[int]struct {
		expectedBinaryValue string
		expectedUint32Value uint32
	}{
		0: {
			expectedBinaryValue: "0",
			expectedUint32Value: 0,
		},
		1: {
			expectedBinaryValue: "10000000000000000000000",
			expectedUint32Value: 1 << 22,
		},
		2: {
			expectedBinaryValue: "1000000000000000000000",
			expectedUint32Value: 1 << 21,
		},
		3: {
			expectedBinaryValue: "11000000000000000000000",
			expectedUint32Value: 3 << 21,
		},
		16: {
			expectedBinaryValue: "1000000000000000000",
			expectedUint32Value: 1 << 18,
		},
		17: {
			expectedBinaryValue: "11000000000000000000",
			expectedUint32Value: 3 << 18,
		},
		18: {
			expectedBinaryValue: "101000000000000000000",
			expectedUint32Value: 5 << 18,
		},
		19: {
			expectedBinaryValue: "111000000000000000000",
			expectedUint32Value: 7 << 18,
		},
		20: {
			expectedBinaryValue: "1001000000000000000000",
			expectedUint32Value: 9 << 18,
		},
		21: {
			expectedBinaryValue: "1011000000000000000000",
			expectedUint32Value: 11 << 18,
		},
		22: {
			expectedBinaryValue: "1101000000000000000000",
			expectedUint32Value: 13 << 18,
		},
		23: {
			expectedBinaryValue: "1111000000000000000000",
			expectedUint32Value: 15 << 18,
		},
		24: {
			expectedBinaryValue: "10001000000000000000000",
			expectedUint32Value: 17 << 18,
		},
		25: {
			expectedBinaryValue: "10011000000000000000000",
			expectedUint32Value: 19 << 18,
		},
		26: {
			expectedBinaryValue: "10101000000000000000000",
			expectedUint32Value: 21 << 18,
		},
		27: {
			expectedBinaryValue: "10111000000000000000000",
			expectedUint32Value: 23 << 18,
		},
		28: {
			expectedBinaryValue: "11001000000000000000000",
			expectedUint32Value: 25 << 18,
		},
		29: {
			expectedBinaryValue: "11011000000000000000000",
			expectedUint32Value: 27 << 18,
		},
		30: {
			expectedBinaryValue: "11101000000000000000000",
			expectedUint32Value: 29 << 18,
		},
		31: {
			expectedBinaryValue: "11111000000000000000000",
			expectedUint32Value: 31 << 18,
		},
	}
	td := NewPower2TokenDistributor([]string{}, 512, math.MaxUint32)
	for instanceID, testData := range tests {
		offset := td.calculateOffset(instanceID)
		require.Equal(t, testData.expectedUint32Value, uint32(offset))
		require.Equal(t, testData.expectedBinaryValue, strconv.FormatInt(int64(offset), 2))
	}
}

func TestPower2TokenDistributor_CalculateTokens(t *testing.T) {
	tests := map[string]struct {
		instanceID     int
		zoneID         int
		expectedTokens func() ring.Tokens
		expectedError  error
	}{
		"instanceID 0, zoneID 0": {
			instanceID: 0,
			zoneID:     0,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				for i := 0; i < 512; i++ {
					tokens = append(tokens, uint32(i<<23))
				}
				return tokens
			},
		},
		"instanceID 0, zoneID 1": {
			instanceID: 0,
			zoneID:     1,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				for i := 0; i < 512; i++ {
					tokens = append(tokens, 2+uint32(i<<23))
				}
				return tokens
			},
		},
		"instanceID 0, zoneID 2": {
			instanceID: 0,
			zoneID:     2,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				for i := 0; i < 512; i++ {
					tokens = append(tokens, 4+uint32(i<<23))
				}
				return tokens
			},
		},
		"instanceID 1, zoneID 0": {
			instanceID: 1,
			zoneID:     0,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				offset := 1 << 22
				for i := 0; i < 512; i++ {
					token := uint32(offset + i<<23)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 1, zoneID 1": {
			instanceID: 1,
			zoneID:     1,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				offset := 2 + 1<<22
				for i := 0; i < 512; i++ {
					token := uint32(offset + i<<23)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 1, zoneID 2": {
			instanceID: 1,
			zoneID:     2,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				offset := 4 + 1<<22
				for i := 0; i < 512; i++ {
					token := uint32(offset + i<<23)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 16, zoneID 0": {
			instanceID: 16,
			zoneID:     0,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				offset := 1 << 18
				for i := 0; i < 512; i++ {
					token := uint32(offset + i<<23)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 16, zoneID 1": {
			instanceID: 16,
			zoneID:     1,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				offset := 2 + 1<<18
				for i := 0; i < 512; i++ {
					token := uint32(offset + i<<23)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 16, zoneID 2": {
			instanceID: 16,
			zoneID:     2,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				offset := 4 + 1<<18
				for i := 0; i < 512; i++ {
					token := uint32(offset + i<<23)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 1<<18, zoneID 0": {
			instanceID: 1 << 18,
			zoneID:     0,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				offset := 1 << 4
				for i := 0; i < 512; i++ {
					token := uint32(offset + i<<23)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 1<<18, zoneID 1": {
			instanceID: 1 << 18,
			zoneID:     1,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				offset := 2 + 1<<4
				for i := 0; i < 512; i++ {
					token := uint32(offset + i<<23)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 1<<18, zoneID 2": {
			instanceID: 1 << 18,
			zoneID:     2,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				offset := 4 + 1<<4
				for i := 0; i < 512; i++ {
					token := uint32(offset + i<<23)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 1<<19, zoneID 0": {
			instanceID:    1 << 19,
			zoneID:        0,
			expectedError: errorInstanceIDTooBig(1<<19, 3, 512),
		},
		"instanceID 1<<19, zoneID 1": {
			instanceID:    1 << 19,
			zoneID:        1,
			expectedError: errorInstanceIDTooBig(1<<19, 3, 512),
		},
		"instanceID 1<<19, zoneID 2": {
			instanceID:    1 << 19,
			zoneID:        2,
			expectedError: errorInstanceIDTooBig(1<<19, 3, 512),
		},
	}
	td := NewPower2TokenDistributor([]string{"zone-a", "zone-b", "zone-c"}, 512, math.MaxUint32)
	for _, testData := range tests {
		tokens, err := td.CalculateTokens(testData.instanceID, testData.zoneID, nil)
		if testData.expectedError == nil {
			require.NoError(t, err)
			require.True(t, testData.expectedTokens().Equals(tokens))
			require.True(t, slices.IsSorted(tokens))
		} else {
			require.Error(t, err)
			require.Equal(t, testData.expectedError, err)
		}
	}
}

func TestPower2TokenDistributor_TokensAreUnique(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 120
	tokensPerInstance := 512
	td := NewPower2TokenDistributor(zones, tokensPerInstance, math.MaxUint32)
	allTokens := make(ring.Tokens, 0, instancesPerZone*len(zones)*tokensPerInstance)
	for i := 0; i < instancesPerZone; i++ {
		for z := range zones {
			tokens, err := td.CalculateTokens(i, z, allTokens)
			require.NoError(t, err)
			for _, token := range tokens {
				require.False(t, slices.Contains(allTokens, token))
				//fmt.Printf("\tinstance id: %5d, zone id: %3d, token id: %d, token: %032b (%10d)\n", i, z, tk, token, token)
				allTokens = append(allTokens, token)
			}
		}
	}
}

func TestRandomTokenDistributor_CalculateTokens(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 120
	tokensPerInstance := 512
	td := NewRandomTokenDistributor(zones, tokensPerInstance, math.MaxUint32)
	allTokens := make(ring.Tokens, 0, instancesPerZone*len(zones)*tokensPerInstance)
	for i := 0; i < instancesPerZone; i++ {
		for z := range zones {
			tokens, err := td.CalculateTokens(i, z, allTokens)
			require.NoError(t, err)
			require.True(t, slices.IsSorted(tokens))
		}
	}
}

func TestRandomTokenDistributor_TokensAreUnique(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 120
	tokensPerInstance := 512
	td := NewRandomTokenDistributor(zones, tokensPerInstance, math.MaxUint32)
	allTokens := make(ring.Tokens, 0, instancesPerZone*len(zones)*tokensPerInstance)
	for i := 0; i < instancesPerZone; i++ {
		for z := range zones {
			tokens, err := td.CalculateTokens(i, z, allTokens)
			require.NoError(t, err)
			for _, token := range tokens {
				require.False(t, slices.Contains(allTokens, token))
				//fmt.Printf("\tinstance id: %5d, zone id: %3d, token id: %d, token: %032b (%10d)\n", i, z, tk, token, token)
				allTokens = append(allTokens, token)
			}
		}
	}
}
