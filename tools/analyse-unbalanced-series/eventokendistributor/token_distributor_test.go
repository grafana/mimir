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

func TestSlidingPower2TokenDistributor_CalculateSlidingOffset(t *testing.T) {
	tests := map[string]struct {
		instanceID          int
		tokenID             int
		expectedBinaryValue string
		expectedUint32Value uint32
	}{
		"0,0": {
			instanceID:          0,
			tokenID:             0,
			expectedBinaryValue: "0",
			expectedUint32Value: 0,
		},
		"0,1": {
			instanceID:          0,
			tokenID:             1,
			expectedBinaryValue: "0",
			expectedUint32Value: 0,
		},
		"1, 0": {
			instanceID:          1,
			tokenID:             0,
			expectedBinaryValue: "10000000000000000000000",
			expectedUint32Value: 1 << 22,
		},
		"1, 1": {
			instanceID:          1,
			tokenID:             1,
			expectedBinaryValue: "10000000000000000000000",
			expectedUint32Value: 1 << 22,
		},
		"2, 0": {
			instanceID:          2,
			tokenID:             0,
			expectedBinaryValue: "1000000000000000000000",
			expectedUint32Value: 1 << 21,
		},
		"2, 1": {
			instanceID:          2,
			tokenID:             1,
			expectedBinaryValue: "11000000000000000000000",
			expectedUint32Value: 3 << 21,
		},
		"2, 7": {
			instanceID:          2,
			tokenID:             7,
			expectedBinaryValue: "11000000000000000000000",
			expectedUint32Value: 3 << 21,
		},
		"2, 8": {
			instanceID:          2,
			tokenID:             8,
			expectedBinaryValue: "1000000000000000000000",
			expectedUint32Value: 1 << 21,
		},
		"3, 0": {
			instanceID:          3,
			tokenID:             0,
			expectedBinaryValue: "11000000000000000000000",
			expectedUint32Value: 3 << 21,
		},
		"3, 1": {
			instanceID:          3,
			tokenID:             1,
			expectedBinaryValue: "1000000000000000000000",
			expectedUint32Value: 1 << 21,
		},
		"3, 7": {
			instanceID:          3,
			tokenID:             7,
			expectedBinaryValue: "1000000000000000000000",
			expectedUint32Value: 1 << 21,
		},
		"3, 8": {
			instanceID:          3,
			tokenID:             8,
			expectedBinaryValue: "11000000000000000000000",
			expectedUint32Value: 3 << 21,
		},
		"16, 0": {
			instanceID:          16,
			tokenID:             0,
			expectedBinaryValue: "1000000000000000000",
			expectedUint32Value: 1 << 18,
		},
		"16, 1": {
			instanceID:          16,
			tokenID:             1,
			expectedBinaryValue: "11000000000000000000",
			expectedUint32Value: 3 << 18,
		},
		"16, 15": {
			instanceID:          16,
			tokenID:             15,
			expectedBinaryValue: "11111000000000000000000",
			expectedUint32Value: 31 << 18,
		},
		"16, 16": {
			instanceID:          16,
			tokenID:             16,
			expectedBinaryValue: "1000000000000000000",
			expectedUint32Value: 1 << 18,
		},
		"17, 0": {
			instanceID:          17,
			tokenID:             0,
			expectedBinaryValue: "11000000000000000000",
			expectedUint32Value: 3 << 18,
		},
		"17, 1": {
			instanceID:          17,
			tokenID:             1,
			expectedBinaryValue: "101000000000000000000",
			expectedUint32Value: 5 << 18,
		},
		"17, 14": {
			instanceID:          17,
			tokenID:             14,
			expectedBinaryValue: "11111000000000000000000",
			expectedUint32Value: 31 << 18,
		},
		"17, 15": {
			instanceID:          17,
			tokenID:             15,
			expectedBinaryValue: "1000000000000000000",
			expectedUint32Value: 1 << 18,
		},
		"18, 0": {
			instanceID:          18,
			tokenID:             0,
			expectedBinaryValue: "101000000000000000000",
			expectedUint32Value: 5 << 18,
		},
		"18, 1": {
			instanceID:          18,
			tokenID:             1,
			expectedBinaryValue: "111000000000000000000",
			expectedUint32Value: 7 << 18,
		},
		"18, 13": {
			instanceID:          18,
			tokenID:             13,
			expectedBinaryValue: "11111000000000000000000",
			expectedUint32Value: 31 << 18,
		},
		"18, 14": {
			instanceID:          18,
			tokenID:             14,
			expectedBinaryValue: "1000000000000000000",
			expectedUint32Value: 1 << 18,
		},
		"31, 0": {
			instanceID:          31,
			tokenID:             0,
			expectedBinaryValue: "11111000000000000000000",
			expectedUint32Value: 31 << 18,
		},
		"31, 1": {
			instanceID:          31,
			tokenID:             1,
			expectedBinaryValue: "1000000000000000000",
			expectedUint32Value: 1 << 18,
		},
		"31, 2": {
			instanceID:          31,
			tokenID:             2,
			expectedBinaryValue: "11000000000000000000",
			expectedUint32Value: 3 << 18,
		},
	}
	td := NewSlidingPower2TokenDistributor([]string{}, 512, math.MaxUint32)
	for _, testData := range tests {
		offset := td.calculateSlidingOffset(testData.tokenID, testData.instanceID)
		require.Equal(t, testData.expectedUint32Value, uint32(offset))
		require.Equal(t, testData.expectedBinaryValue, strconv.FormatInt(int64(offset), 2))
	}
}

func TestSlidingPower2TokenDistributor_CalculateTokens(t *testing.T) {
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
				for i := 0; i < 512; i++ {
					slidingOffset := (((16+i)%16)*2 + 1) << 18
					token := uint32(slidingOffset + i<<23)
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
				zoneOffset := 2
				for i := 0; i < 512; i++ {
					slidingOffset := (((16+i)%16)*2 + 1) << 18
					token := uint32(slidingOffset + i<<23 + zoneOffset)
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
				zoneOffset := 4
				for i := 0; i < 512; i++ {
					slidingOffset := (((16+i)%16)*2 + 1) << 18
					token := uint32(slidingOffset + i<<23 + zoneOffset)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 30, zoneID 0": {
			instanceID: 30,
			zoneID:     0,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				for i := 0; i < 512; i++ {
					slidingOffset := (((30+i)%16)*2 + 1) << 18
					token := uint32(slidingOffset + i<<23)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 30, zoneID 1": {
			instanceID: 30,
			zoneID:     1,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				zoneOffset := 2
				for i := 0; i < 512; i++ {
					slidingOffset := (((30+i)%16)*2 + 1) << 18
					token := uint32(slidingOffset + i<<23 + zoneOffset)
					tokens = append(tokens, token)
				}
				return tokens
			},
		},
		"instanceID 30, zoneID 2": {
			instanceID: 30,
			zoneID:     2,
			expectedTokens: func() ring.Tokens {
				tokens := make(ring.Tokens, 0, 512)
				zoneOffset := 4
				for i := 0; i < 512; i++ {
					slidingOffset := (((30+i)%16)*2 + 1) << 18
					token := uint32(slidingOffset + i<<23 + zoneOffset)
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
				instanceID := 1 << 18
				for i := 0; i < 512; i++ {
					slidingOffset := (((instanceID+i)%instanceID)*2 + 1) << 4
					token := uint32(slidingOffset + i<<23)
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
				instanceID := 1 << 18
				zoneOffset := 2
				for i := 0; i < 512; i++ {
					slidingOffset := (((instanceID+i)%instanceID)*2 + 1) << 4
					token := uint32(slidingOffset + i<<23 + zoneOffset)
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
				instanceID := 1 << 18
				zoneOffset := 4
				for i := 0; i < 512; i++ {
					slidingOffset := (((instanceID+i)%instanceID)*2 + 1) << 4
					token := uint32(slidingOffset + i<<23 + zoneOffset)
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
	td := NewSlidingPower2TokenDistributor([]string{"zone-a", "zone-b", "zone-c"}, 512, math.MaxUint32)
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

func TestSlidingPower2TokenDistributor_TokensAreUnique(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 120
	tokensPerInstance := 512
	td := NewSlidingPower2TokenDistributor(zones, tokensPerInstance, math.MaxUint32)
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
	td := NewRandomTokenDistributor(tokensPerInstance, math.MaxUint32)
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
	td := NewRandomTokenDistributor(tokensPerInstance, math.MaxUint32)
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

func TestRandomBucketTokenDistributor_CalculateTokens(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 120
	tokensPerInstance := 512
	tokenDistance := 1 << 32 / tokensPerInstance
	td := NewRandomBucketTokenDistributor(tokensPerInstance, math.MaxUint32)
	allTokens := make(ring.Tokens, 0, instancesPerZone*len(zones)*tokensPerInstance)
	for i := 0; i < instancesPerZone; i++ {
		for z := range zones {
			tokens, err := td.CalculateTokens(i, z, allTokens)
			require.NoError(t, err)
			require.True(t, slices.IsSorted(tokens))
			for tk, token := range tokens {
				require.True(t, token >= uint32(tk*tokenDistance))
				if tk == tokensPerInstance-1 {
					require.True(t, token <= math.MaxUint32)
				} else {
					require.True(t, token < uint32((tk+1)*tokenDistance))
				}
			}
		}
	}
}

func TestRandomBucketTokenDistributor_TokensAreUnique(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 120
	tokensPerInstance := 512
	td := NewRandomBucketTokenDistributor(tokensPerInstance, math.MaxUint32)
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

func TestRandomModuloTokenDistributor_CalculateTokens(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 120
	tokensPerInstance := 512
	tokenDistance := 1 << 32 / tokensPerInstance
	td := NewRandomModuloTokenDistributor(zones, tokensPerInstance, math.MaxUint32)
	allTokens := make(ring.Tokens, 0, instancesPerZone*len(zones)*tokensPerInstance)
	for i := 0; i < instancesPerZone; i++ {
		for z := range zones {
			tokens, err := td.CalculateTokens(i, z, allTokens)
			require.NoError(t, err)
			require.True(t, slices.IsSorted(tokens))
			for tk, token := range tokens {
				require.True(t, token >= uint32(tk*tokenDistance))
				require.Equal(t, z, int(token)%len(zones))
				if tk == tokensPerInstance-1 {
					require.True(t, token <= math.MaxUint32)
				} else {
					require.True(t, token < uint32((tk+1)*tokenDistance))
				}
			}
		}
	}
}

func TestRandomModuloTokenDistributor_TokensAreUnique(t *testing.T) {
	zones := []string{"zone-a", "zone-b", "zone-c"}
	instancesPerZone := 120
	tokensPerInstance := 512
	td := NewRandomModuloTokenDistributor(zones, tokensPerInstance, math.MaxUint32)
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

func TestRandomModuloTokenDistributor_FindToken(t *testing.T) {
	tests := map[string]struct {
		tokens        ring.Tokens
		index         int
		zoneID        int
		expectedToken uint32
	}{
		"zoneID==0, index==0": {
			tokens:        ring.Tokens([]uint32{48, 378, 570, 900}),
			index:         0,
			zoneID:        0,
			expectedToken: 972,
		},
		"zoneID==0, index!=0": {
			tokens:        ring.Tokens([]uint32{48, 378, 570, 900}),
			index:         1,
			zoneID:        0,
			expectedToken: 213,
		},
		"zoneID==1, index==0": {
			tokens:        ring.Tokens([]uint32{49, 379, 571, 901}),
			index:         0,
			zoneID:        1,
			expectedToken: 976,
		},
		"zoneID==1, index!=0": {
			tokens:        ring.Tokens([]uint32{49, 379, 571, 901}),
			index:         1,
			zoneID:        1,
			expectedToken: 214,
		},
		"zoneID==2, index==0": {
			tokens:        ring.Tokens([]uint32{50, 380, 572, 902}),
			index:         0,
			zoneID:        2,
			expectedToken: 977,
		},
		"zoneID==2, index!=0": {
			tokens:        ring.Tokens([]uint32{50, 380, 572, 902}),
			index:         1,
			zoneID:        2,
			expectedToken: 215,
		},
	}
	zones := []string{"zone-a", "zone-b", "zone-c"}
	//instancesPerZone := 5
	tokensPerInstance := 4
	maxToken := uint32(1000)
	td := NewRandomModuloTokenDistributor(zones, tokensPerInstance, maxToken)
	/*tokensByZone := make(map[string]ring.Tokens, len(zones))
	for _, zone := range zones {
		tokensByZone[zone] = make(ring.Tokens, 0, instancesPerZone*tokensPerInstance)
	}
	allTokens := make(ring.Tokens, 0, instancesPerZone*len(zones)*tokensPerInstance)
	for i := 0; i < instancesPerZone; i++ {
		for z, zone := range zones {
			tokens, err := td.CalculateTokens(i, z, allTokens)
			require.NoError(t, err)
			for _, token := range tokens {
				require.False(t, slices.Contains(allTokens, token))
				//fmt.Printf("\tinstance id: %5d, zone id: %3d, token id: %d, token: %032b (%10d)\n", i, z, tk, token, token)
				allTokens = append(allTokens, token)
			}
			tokensByZone[zone] = append(tokensByZone[zone], tokens...)
		}
	}

	for _, zone := range zones {
		slices.Sort(tokensByZone[zone])
	}*/
	for _, testData := range tests {
		tokens := testData.tokens
		token := tokens[testData.index]
		prevIndex := testData.index - 1
		if prevIndex < 0 {
			prevIndex = len(tokens) - 1
		}
		dist := float64(distance(tokens[prevIndex], token, maxToken))
		oi := newOwnershipInfo(dist)
		oi.SetRingItem(NewRingToken(token, tokens[prevIndex]))
		next := td.findToken(oi, tokens, testData.zoneID)
		require.Equal(t, testData.expectedToken, next)
		require.Equal(t, uint32(testData.zoneID), next%uint32(len(zones)))
	}
}
