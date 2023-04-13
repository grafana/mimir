package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRegisteredTokensOwnership(t *testing.T) {
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

	actual := getRegisteredTokensOwnership(ringTokens, ringInstanceByToken)
	expected := []ingesterOwnership{
		{id: "ingester-1", percentage: (4000000.0 / math.MaxUint32) * 100},
		{id: "ingester-2", percentage: (3000000.0 / math.MaxUint32) * 100},
		{id: "ingester-3", percentage: ((2000000.0 + (math.MaxUint32 - 9000000.0)) / math.MaxUint32) * 100},
	}

	assert.Equal(t, expected, actual)
}
