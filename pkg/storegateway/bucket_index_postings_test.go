// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestBigEndianPostingsCount(t *testing.T) {
	const count = 1000
	raw := make([]byte, count*4)

	for ix := 0; ix < count; ix++ {
		binary.BigEndian.PutUint32(raw[4*ix:], rand.Uint32())
	}

	p := newBigEndianPostings(raw)
	assert.Equal(t, count, p.length())

	c := 0
	for p.Next() {
		c++
	}
	assert.Equal(t, count, c)
}

func TestWorstCaseFetchedDataStrategy(t *testing.T) {
	testCases := map[string]struct {
		input            []postingGroup
		expectedSelected []postingGroup
		expectedOmitted  []postingGroup
	}{
		"single posting group is selected": {
			input: []postingGroup{
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 128},
			},
		},
		"only all-postings & subtracting groups": {
			input: []postingGroup{
				{totalSize: 0 /* all-postings doesn't have a size at the moment */, keys: []labels.Label{allPostingsKey}},
				{isSubtract: true, totalSize: 128},
				{isSubtract: true, totalSize: 64 * 1024 * 1024},
			},
			expectedSelected: []postingGroup{
				{totalSize: 0, keys: []labels.Label{allPostingsKey}},
				{isSubtract: true, totalSize: 128},
				{isSubtract: true, totalSize: 64 * 1024 * 1024},
			},
		},
		"only small posting lists": {
			input: []postingGroup{
				{totalSize: 1024},
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 1024},
				{totalSize: 256},
				{totalSize: 128},
			},
		},
		"two small and one large list": {
			input: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedOmitted: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
			},
		},
		"if smallest group is subtractive it's not used as min size": {
			input: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
				{totalSize: 1024 * 1024},
				{isSubtract: true, totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
				{totalSize: 1024 * 1024},
				{isSubtract: true, totalSize: 128},
			},
		},
		"one small and two mid size lists are all selected": {
			input: []postingGroup{
				{totalSize: 128},
				{totalSize: 4 * 1024},
				{totalSize: 4 * 1024},
			},
			expectedSelected: []postingGroup{
				{totalSize: 128},
				{totalSize: 4 * 1024},
				{totalSize: 4 * 1024},
			},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			actualSelected, actualOmitted := worstCaseFetchedDataStrategy{1.0}.selectPostings(testCase.input)
			assert.ElementsMatch(t, testCase.expectedSelected, actualSelected)
			assert.ElementsMatch(t, testCase.expectedOmitted, actualOmitted)
		})
	}
}

func TestSpeculativeFetchedDataStrategy(t *testing.T) {
	testCases := map[string]struct {
		input            []postingGroup
		expectedSelected []postingGroup
		expectedOmitted  []postingGroup
	}{
		"single posting group is selected": {
			input: []postingGroup{
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 128},
			},
		},
		"only all-postings & subtracting groups": {
			input: []postingGroup{
				{totalSize: 0 /* all-postings doesn't have a size at the moment */, keys: []labels.Label{allPostingsKey}},
				{isSubtract: true, totalSize: 128},
				{isSubtract: true, totalSize: 64 * 1024 * 1024},
			},
			expectedSelected: []postingGroup{
				{totalSize: 0, keys: []labels.Label{allPostingsKey}},
				{isSubtract: true, totalSize: 128},
				{isSubtract: true, totalSize: 64 * 1024 * 1024},
			},
		},
		"only small posting lists": {
			input: []postingGroup{
				{totalSize: 1024},
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 1024},
				{totalSize: 256},
				{totalSize: 128},
			},
		},
		"two small and one large list": {
			input: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedOmitted: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
			},
		},
		"one small and two mid size lists - last is omitted (more aggressive than worstCase)": {
			input: []postingGroup{
				{totalSize: 128},
				{totalSize: 4 * 1024},
				{totalSize: 4 * 1024},
			},
			expectedSelected: []postingGroup{
				{totalSize: 128},
				{totalSize: 4 * 1024},
			},
			expectedOmitted: []postingGroup{
				{totalSize: 4 * 1024},
			},
		},
		"subtractive lists are not taken into consideration for halving number of postings": {
			input: []postingGroup{
				{totalSize: 128},
				{isSubtract: true, totalSize: 256},
				{isSubtract: true, totalSize: 512},
				{totalSize: 4 * 1024},
				{totalSize: 4 * 1024},
			},
			expectedSelected: []postingGroup{
				{totalSize: 128},
				{isSubtract: true, totalSize: 256},
				{isSubtract: true, totalSize: 512},
				{totalSize: 4 * 1024},
			},
			expectedOmitted: []postingGroup{
				{totalSize: 4 * 1024},
			},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			actualSelected, actualOmitted := speculativeFetchedDataStrategy{}.selectPostings(testCase.input)
			assert.ElementsMatch(t, testCase.expectedSelected, actualSelected)
			assert.ElementsMatch(t, testCase.expectedOmitted, actualOmitted)
		})
	}
}
