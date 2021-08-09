// SPDX-License-Identifier: AGPL-3.0-only

package querysharding

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
)

func TestRemoveShardFromMatchers(t *testing.T) {
	tests := map[string]struct {
		input            []*labels.Matcher
		expectedShard    *ShardSelector
		expectedMatchers []*labels.Matcher
		expectedError    error
	}{
		"should return no shard on empty label matchers": {},
		"should return no shard on no shard label matcher": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test"),
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar.*"),
			},
			expectedShard: nil,
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test"),
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar.*"),
			},
		},
		"should return matching shard and filter out its matcher": {
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test"),
				labels.MustNewMatcher(labels.MatchEqual, ShardLabel, ShardSelector{ShardIndex: 1, ShardCount: 8}.Label().Value),
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar.*"),
			},
			expectedShard: &ShardSelector{
				ShardIndex: 1,
				ShardCount: 8,
			},
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test"),
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar.*"),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualShard, actualMatchers, actualError := RemoveShardFromMatchers(testData.input)
			assert.Equal(t, testData.expectedShard, actualShard)
			assert.Equal(t, testData.expectedMatchers, actualMatchers)
			assert.Equal(t, testData.expectedError, actualError)
		})
	}
}
