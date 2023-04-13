package main

import (
	"math"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
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

func TestGenerateRingWithPerfectlySpacedTokens(t *testing.T) {
	now := time.Now()

	mockInstanceDesc := func(addr, zone string, tokens []uint32) ring.InstanceDesc {
		return ring.InstanceDesc{
			Addr:                addr,
			Timestamp:           now.Unix(),
			State:               ring.ACTIVE,
			Tokens:              tokens,
			Zone:                zone,
			RegisteredTimestamp: now.Unix(),
		}
	}

	// To simplify the test and make it easier to understand, we reduce
	// the max token value to 120, which is a multiple of 6 ingesters * 2
	// tokens per ingester.
	actual := generateRingWithPerfectlySpacedTokens(6, 3, 2, 120, now)

	expected := &ring.Desc{
		Ingesters: map[string]ring.InstanceDesc{
			"ingester-zone-a-0": mockInstanceDesc("ingester-zone-a-0", "zone-a", []uint32{0, 60}),
			"ingester-zone-b-0": mockInstanceDesc("ingester-zone-b-0", "zone-b", []uint32{10, 70}),
			"ingester-zone-c-0": mockInstanceDesc("ingester-zone-c-0", "zone-c", []uint32{20, 80}),
			"ingester-zone-a-1": mockInstanceDesc("ingester-zone-a-1", "zone-a", []uint32{30, 90}),
			"ingester-zone-b-1": mockInstanceDesc("ingester-zone-b-1", "zone-b", []uint32{40, 100}),
			"ingester-zone-c-1": mockInstanceDesc("ingester-zone-c-1", "zone-c", []uint32{50, 110}),
		},
	}

	assert.Equal(t, expected, actual)
}
