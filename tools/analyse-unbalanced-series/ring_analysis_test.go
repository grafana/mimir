package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRegisteredTokensOwnership(t *testing.T) {
	tests := map[string]struct {
		instances []ringIngesterDesc
		expected  []ingesterOwnership
	}{
		"empty ring": {
			instances: nil,
			expected:  []ingesterOwnership{},
		},
		"1 instance with 1 token in the ring": {
			instances: []ringIngesterDesc{
				{ID: "ingester-1", Tokens: []uint32{1000000}},
			},
			expected: []ingesterOwnership{
				{id: "ingester-1", percentage: 100},
			},
		},
		"1 instance with multiple tokens in the ring": {
			instances: []ringIngesterDesc{
				{ID: "ingester-1", Tokens: []uint32{1000000, 2000000, 3000000}},
			},
			expected: []ingesterOwnership{
				{id: "ingester-1", percentage: 100},
			},
		},
		"multiple instances with multiple tokens in the ring": {
			instances: []ringIngesterDesc{
				{ID: "ingester-1", Tokens: []uint32{1000000, 3000000, 6000000}},
				{ID: "ingester-2", Tokens: []uint32{2000000, 4000000, 8000000}},
				{ID: "ingester-3", Tokens: []uint32{5000000, 9000000}},
			},
			expected: []ingesterOwnership{
				{id: "ingester-1", percentage: ((3000000.0 + (math.MaxUint32 - 9000000.0)) / math.MaxUint32) * 100},
				{id: "ingester-2", percentage: (4000000.0 / math.MaxUint32) * 100},
				{id: "ingester-3", percentage: (2000000.0 / math.MaxUint32) * 100},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ringStatus := &ringStatusDesc{
				Ingesters: testData.instances,
			}

			ringDesc := ringStatus.toRingModel()
			ringTokens := ringDesc.GetTokens()
			ringInstanceByToken := getRingInstanceByToken(ringDesc)

			actual := getRegisteredTokensOwnership(ringTokens, ringInstanceByToken)
			assert.Equal(t, testData.expected, actual)
		})
	}

}

func TestSearchToken(t *testing.T) {
	ringStatus := &ringStatusDesc{
		Ingesters: []ringIngesterDesc{
			{ID: "ingester-1", Tokens: []uint32{1000000, 3000000, 6000000}},
			{ID: "ingester-2", Tokens: []uint32{2000000, 4000000, 8000000}},
			{ID: "ingester-3", Tokens: []uint32{5000000, 9000000}},
		},
	}

	ringDesc := ringStatus.toRingModel()
	ringTokens := ringDesc.GetTokens()
	ringInstanceByToken := getRingInstanceByToken(ringDesc)

	offset := searchToken(ringTokens, 2000000+1)
	assert.Equal(t, "ingester-1", ringInstanceByToken[ringTokens[offset]].InstanceID)

	offset = searchToken(ringTokens, 2000000)
	assert.Equal(t, "ingester-1", ringInstanceByToken[ringTokens[offset]].InstanceID)

	offset = searchToken(ringTokens, 1500000)
	assert.Equal(t, "ingester-2", ringInstanceByToken[ringTokens[offset]].InstanceID)

	offset = searchToken(ringTokens, 1000000)
	assert.Equal(t, "ingester-2", ringInstanceByToken[ringTokens[offset]].InstanceID)

	offset = searchToken(ringTokens, 1000000-1)
	assert.Equal(t, "ingester-1", ringInstanceByToken[ringTokens[offset]].InstanceID)

	offset = searchToken(ringTokens, 0)
	assert.Equal(t, "ingester-1", ringInstanceByToken[ringTokens[offset]].InstanceID)
}
