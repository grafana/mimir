// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostingValueOffsets(t *testing.T) {
	testCases := map[string]struct {
		existingOffsets []postingOffset
		prefix          string
		expectedFound   bool
		expectedStart   int
		expectedEnd     int
	}{
		"prefix not found": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "a",
			expectedFound: false,
		},
		"prefix matches only one sampled offset": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "02",
			expectedFound: true,
			expectedStart: 1,
			expectedEnd:   2,
		},
		"prefix matches all offsets": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "0",
			expectedFound: true,
			expectedStart: 0,
			expectedEnd:   4,
		},
		"prefix matches only last offset": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "031",
			expectedFound: true,
			expectedStart: 3,
			expectedEnd:   4,
		},
		"prefix matches multiple offsets": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "020"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "02",
			expectedFound: true,
			expectedStart: 1,
			expectedEnd:   3,
		},
		"prefix matches only first offset": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "020"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "015",
			expectedFound: true,
			expectedStart: 0,
			expectedEnd:   1,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			offsets := postingValueOffsets{offsets: testCase.existingOffsets}
			start, end, found := offsets.prefixOffsets(testCase.prefix)
			assert.Equal(t, testCase.expectedStart, start)
			assert.Equal(t, testCase.expectedEnd, end)
			assert.Equal(t, testCase.expectedFound, found)
		})
	}
}
