// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeProjectionLabels(t *testing.T) {
	testCases := map[string]struct {
		firstInclude    bool
		firstLabels     []string
		secondInclude   bool
		secondLabels    []string
		expectedInclude bool
		expectedLabels  []string
	}{
		"different includes": {
			firstInclude:    true,
			firstLabels:     []string{"job"},
			secondInclude:   false,
			secondLabels:    []string{"pod"},
			expectedInclude: false,
			expectedLabels:  []string{},
		},
		"include some labels and include no labels": {
			firstInclude:    true,
			firstLabels:     nil,
			secondInclude:   true,
			secondLabels:    []string{"zone"},
			expectedInclude: true,
			expectedLabels:  []string{"zone"},
		},
		"include some labels and include some labels": {
			firstInclude:    true,
			firstLabels:     []string{"zone"},
			secondInclude:   true,
			secondLabels:    []string{"region"},
			expectedInclude: true,
			expectedLabels:  []string{"region", "zone"},
		},
		"exclude some labels and exclude no labels": {
			firstInclude:    false,
			firstLabels:     []string{"instance"},
			secondInclude:   false,
			secondLabels:    nil,
			expectedInclude: false,
			expectedLabels:  []string{},
		},
		"exclude some labels and exclude some labels no overlap": {
			firstInclude:    false,
			firstLabels:     []string{"instance"},
			secondInclude:   false,
			secondLabels:    []string{"pod"},
			expectedInclude: false,
			expectedLabels:  []string{},
		},
		"exclude some labels and exclude some labels with overlap": {
			firstInclude:    false,
			firstLabels:     []string{"instance", "host", "zone"},
			secondInclude:   false,
			secondLabels:    []string{"host", "pod", "region"},
			expectedInclude: false,
			expectedLabels:  []string{"host"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {

			include, lbls := mergeProjectionLabels(tc.firstInclude, tc.firstLabels, tc.secondInclude, tc.secondLabels)
			require.Equal(t, tc.expectedInclude, include)
			require.Equal(t, tc.expectedLabels, lbls)
		})
	}
}
