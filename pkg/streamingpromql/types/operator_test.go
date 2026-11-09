// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"slices"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

var (
	clusterMatcher       = Matcher{Type: labels.MatchRegexp, Name: "cluster", Value: "a|b"}
	clusterNotEqMatcher  = Matcher{Type: labels.MatchNotEqual, Name: "cluster", Value: "c"}
	namespaceMatcher     = Matcher{Type: labels.MatchEqual, Name: "namespace", Value: "ns"}
	podMatcher           = Matcher{Type: labels.MatchRegexp, Name: "pod", Value: "|pod-1"}
	matchersForFiltering = Matchers{clusterMatcher, namespaceMatcher, clusterNotEqMatcher, podMatcher}
)

func TestMatchers_With(t *testing.T) {
	testCases := map[string]struct {
		matchers Matchers
		names    []string
		expected Matchers
	}{
		"no matchers": {
			matchers: nil,
			names:    []string{"cluster"},
			expected: Matchers{},
		},
		"no names": {
			matchers: matchersForFiltering,
			names:    nil,
			expected: Matchers{},
		},
		"no matchers for the names": {
			matchers: matchersForFiltering,
			names:    []string{"job"},
			expected: Matchers{},
		},
		"one name with more than one matcher": {
			matchers: matchersForFiltering,
			names:    []string{"cluster"},
			expected: Matchers{clusterMatcher, clusterNotEqMatcher},
		},
		"more than one name, in a different order to the matchers": {
			matchers: matchersForFiltering,
			names:    []string{"pod", "cluster"},
			expected: Matchers{clusterMatcher, clusterNotEqMatcher, podMatcher},
		},
		"all names": {
			matchers: matchersForFiltering,
			names:    []string{"cluster", "namespace", "pod"},
			expected: matchersForFiltering,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			original := slices.Clone(testCase.matchers)
			require.Equal(t, testCase.expected, testCase.matchers.With(testCase.names...))
			require.Equal(t, original, testCase.matchers, "the original matchers must not change")
		})
	}
}

func TestMatchers_Without(t *testing.T) {
	testCases := map[string]struct {
		matchers Matchers
		names    []string
		expected Matchers
	}{
		"no matchers": {
			matchers: nil,
			names:    []string{"cluster"},
			expected: Matchers{},
		},
		"no names": {
			matchers: matchersForFiltering,
			names:    nil,
			expected: matchersForFiltering,
		},
		"no matchers for the names": {
			matchers: matchersForFiltering,
			names:    []string{"job"},
			expected: matchersForFiltering,
		},
		"one name with more than one matcher": {
			matchers: matchersForFiltering,
			names:    []string{"cluster"},
			expected: Matchers{namespaceMatcher, podMatcher},
		},
		"more than one name, in a different order to the matchers": {
			matchers: matchersForFiltering,
			names:    []string{"pod", "cluster"},
			expected: Matchers{namespaceMatcher},
		},
		"all names": {
			matchers: matchersForFiltering,
			names:    []string{"cluster", "namespace", "pod"},
			expected: Matchers{},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			original := slices.Clone(testCase.matchers)
			require.Equal(t, testCase.expected, testCase.matchers.Without(testCase.names...))
			require.Equal(t, original, testCase.matchers, "the original matchers must not change")
		})
	}
}
