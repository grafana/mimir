// SPDX-License-Identifier: AGPL-3.0-only

package querysharding

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseShard(t *testing.T) {
	tests := map[string]struct {
		input  string
		output ShardSelector
		err    bool
	}{
		"should return error on invalid format": {
			input:  "lsdjf",
			output: ShardSelector{},
			err:    true,
		},
		"should return error on invalid index (not an integer)": {
			input:  "a_of_3",
			output: ShardSelector{},
			err:    true,
		},
		"should return error on invalid index (not positive)": {
			input:  "-1_of_3",
			output: ShardSelector{},
			err:    true,
		},
		"should return error on invalid index (too large)": {
			input:  "3_of_3",
			output: ShardSelector{},
			err:    true,
		},
		"should return error on invalid separator": {
			input:  "1_out_3",
			output: ShardSelector{},
			err:    true,
		},
		"should succeed on valid shard selector": {
			input: "1_of_2",
			output: ShardSelector{
				ShardIndex: 1,
				ShardCount: 2,
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			shard, err := parseShard(testData.input)
			if testData.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, testData.output, shard)
			}
		})
	}
}

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
				labels.MustNewMatcher(labels.MatchEqual, ShardLabel, ShardSelector{ShardIndex: 1, ShardCount: 8}.LabelValue()),
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

func TestShardFromMatchers(t *testing.T) {
	testExpr := []struct {
		input []*labels.Matcher
		shard *ShardSelector
		idx   int
		err   bool
	}{
		{
			input: []*labels.Matcher{
				{},
				{
					Name: ShardLabel,
					Type: labels.MatchEqual,
					Value: ShardSelector{
						ShardIndex: 10,
						ShardCount: 16,
					}.LabelValue(),
				},
				{},
			},
			shard: &ShardSelector{
				ShardIndex: 10,
				ShardCount: 16,
			},
			idx: 1,
			err: false,
		},
		{
			input: []*labels.Matcher{
				{
					Name:  ShardLabel,
					Type:  labels.MatchEqual,
					Value: "invalid-fmt",
				},
			},
			shard: nil,
			idx:   0,
			err:   true,
		},
		{
			input: []*labels.Matcher{},
			shard: nil,
			idx:   0,
			err:   false,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			shard, idx, err := ShardFromMatchers(c.input)
			if c.err {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				require.Equal(t, c.shard, shard)
				require.Equal(t, c.idx, idx)
			}
		})
	}
}
