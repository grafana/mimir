// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"fmt"
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

func Test_PostingValueOffsets_Downsample(t *testing.T) {

	pvos := make([]postingOffset, 20)
	for i := range pvos {
		pvos[i] = postingOffset{value: fmt.Sprintf("%d", i)}
	}

	tests := []struct {
		name        string
		pvo         []postingOffset
		step        int
		expectedLen int
	}{
		{"len_eq_when_step_size=1", pvos, 1, 20},
		{"len_approx_halved_when_step_size=2", pvos, 2, 10},
		{"len_approx_quartered_when_step_size=4", pvos, 4, 5},
		{"len_one_when_step_size_is_len_offsets", pvos, 20, 1},
		{"len_one_when_step_size_exceeds_len_offsets", pvos, 200, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pvo := &postingValueOffsets{offsets: append([]postingOffset{}, tc.pvo...)}
			pvo.downsample(tc.step)
			assert.Equal(t, tc.expectedLen, len(pvo.offsets), "unexpected length for step size %d", tc.step)
		})
	}
}
