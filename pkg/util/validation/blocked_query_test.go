// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

func TestBlockedQueriesConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		input          BlockedQueriesConfig
		expectedErrMsg string
	}{
		"no rules": {},
		"literal pattern": {
			input: BlockedQueriesConfig{
				{Pattern: "rate(metric_counter[5m])", Regex: false},
			},
			expectedErrMsg: "", // none
		},
		"empty pattern": {
			input: BlockedQueriesConfig{
				{TimeRangeLongerThan: model.Duration(24 * time.Hour)},
			},
			expectedErrMsg: "blocked_queries[0]: pattern is required",
		},
		"empty pattern second rule": {
			input: BlockedQueriesConfig{
				{Pattern: "rate(metric_counter[5m])", Regex: false},
				{Pattern: "", TimeRangeLongerThan: model.Duration(24 * time.Hour)},
			},
			expectedErrMsg: "blocked_queries[1]: pattern is required",
		},
		"valid regex": {
			input: BlockedQueriesConfig{
				{Pattern: ".*expensive.*", Regex: true},
			},
			expectedErrMsg: "", // none
		},
		"invalid regex": {
			input: BlockedQueriesConfig{
				{Pattern: "[a-9}", Regex: true},
			},
			expectedErrMsg: `blocked_queries[0]: invalid regex pattern "[a-9}"`,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := tc.input.Validate()
			if tc.expectedErrMsg == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.expectedErrMsg)
			}
		})
	}
}

func TestBlockedQuery_MatchesRule(t *testing.T) {
	tests := map[string]struct {
		rule            BlockedQuery
		input           BlockedQueryInput
		expectedMatches bool
	}{
		// Pattern matching
		"empty pattern returns false": {
			rule:            BlockedQuery{},
			input:           BlockedQueryInput{Query: "up"},
			expectedMatches: false,
		},
		"whitespace-only pattern returns false": {
			rule:            BlockedQuery{Pattern: "   "},
			input:           BlockedQueryInput{Query: "up"},
			expectedMatches: false,
		},
		"literal match": {
			rule:            BlockedQuery{Pattern: "rate(metric_counter[5m])"},
			input:           BlockedQueryInput{Query: "rate(metric_counter[5m])"},
			expectedMatches: true,
		},
		"literal no match": {
			rule:            BlockedQuery{Pattern: "rate(metric_counter[5m])"},
			input:           BlockedQueryInput{Query: "rate(other[5m])"},
			expectedMatches: false,
		},
		"literal match with surrounding whitespace in pattern": {
			rule:            BlockedQuery{Pattern: "  rate(metric_counter[5m])  "},
			input:           BlockedQueryInput{Query: "rate(metric_counter[5m])"},
			expectedMatches: true,
		},
		"literal match with surrounding whitespace in query": {
			rule:            BlockedQuery{Pattern: "rate(metric_counter[5m])"},
			input:           BlockedQueryInput{Query: "  rate(metric_counter[5m])  "},
			expectedMatches: true,
		},
		"regex match": {
			rule:            BlockedQuery{Pattern: ".*metric_counter.*", Regex: true},
			input:           BlockedQueryInput{Query: "rate(metric_counter[5m])"},
			expectedMatches: true,
		},
		"regex no match": {
			rule:            BlockedQuery{Pattern: ".*other.*", Regex: true},
			input:           BlockedQueryInput{Query: "rate(metric_counter[5m])"},
			expectedMatches: false,
		},
		"bad regex returns false": {
			rule:            BlockedQuery{Pattern: "[a-9}", Regex: true},
			input:           BlockedQueryInput{Query: "rate(metric_counter[5m])"},
			expectedMatches: false,
		},
		"literal also matches when regex: true": {
			rule:            BlockedQuery{Pattern: "rate(metric_counter[5m])", Regex: true},
			input:           BlockedQueryInput{Query: "rate(metric_counter[5m])"},
			expectedMatches: true,
		},

		// UnalignedRangeQueries
		"unaligned_range_queries: pattern no match skips filter": {
			rule:            BlockedQuery{Pattern: "up", UnalignedRangeQueries: true},
			input:           BlockedQueryInput{Query: "rate(metric_counter[5m])", QueryType: QueryTypeRange, StepAligned: false},
			expectedMatches: false,
		},
		"unaligned_range_queries: unaligned range query blocked": {
			rule:            BlockedQuery{Pattern: "up", UnalignedRangeQueries: true},
			input:           BlockedQueryInput{Query: "up", QueryType: QueryTypeRange, StepAligned: false},
			expectedMatches: true,
		},
		"unaligned_range_queries: aligned range query not blocked": {
			rule:            BlockedQuery{Pattern: "up", UnalignedRangeQueries: true},
			input:           BlockedQueryInput{Query: "up", QueryType: QueryTypeRange, StepAligned: true},
			expectedMatches: false,
		},
		"unaligned_range_queries: instant query not blocked": {
			rule:            BlockedQuery{Pattern: "up", UnalignedRangeQueries: true},
			input:           BlockedQueryInput{Query: "up", QueryType: QueryTypeInstant},
			expectedMatches: false,
		},
		"unaligned_range_queries: remote read not blocked": {
			rule:            BlockedQuery{Pattern: "up", UnalignedRangeQueries: true},
			input:           BlockedQueryInput{Query: "up", QueryType: QueryTypeRemoteRead},
			expectedMatches: false,
		},
		"unaligned_range_queries: false does not filter": {
			rule:            BlockedQuery{Pattern: "up", UnalignedRangeQueries: false},
			input:           BlockedQueryInput{Query: "up", QueryType: QueryTypeRange, StepAligned: false},
			expectedMatches: true,
		},

		// TimeRangeLongerThan
		"time_range_longer_than: duration exceeds threshold blocked": {
			rule:            BlockedQuery{Pattern: "up", TimeRangeLongerThan: model.Duration(24 * time.Hour)},
			input:           BlockedQueryInput{Query: "up", QueryDuration: 48 * time.Hour},
			expectedMatches: true,
		},
		"time_range_longer_than: duration equals threshold not blocked": {
			rule:            BlockedQuery{Pattern: "up", TimeRangeLongerThan: model.Duration(24 * time.Hour)},
			input:           BlockedQueryInput{Query: "up", QueryDuration: 24 * time.Hour},
			expectedMatches: false,
		},
		"time_range_longer_than: duration under threshold not blocked": {
			rule:            BlockedQuery{Pattern: "up", TimeRangeLongerThan: model.Duration(24 * time.Hour)},
			input:           BlockedQueryInput{Query: "up", QueryDuration: 12 * time.Hour},
			expectedMatches: false,
		},
		"time_range_longer_than: instant query not blocked": {
			rule:            BlockedQuery{Pattern: "up", TimeRangeLongerThan: model.Duration(24 * time.Hour)},
			input:           BlockedQueryInput{Query: "up", QueryType: QueryTypeInstant},
			expectedMatches: false,
		},
		"time_range_longer_than: zero disables filter": {
			rule:            BlockedQuery{Pattern: "up", TimeRangeLongerThan: 0},
			input:           BlockedQueryInput{Query: "up", QueryDuration: 48 * time.Hour},
			expectedMatches: true,
		},

		// StepSizeShorterThan
		"step_size_shorter_than: step below threshold blocked": {
			rule:            BlockedQuery{Pattern: "up", StepSizeShorterThan: model.Duration(time.Minute)},
			input:           BlockedQueryInput{Query: "up", StepKnown: true, Step: 30 * time.Second},
			expectedMatches: true,
		},
		"step_size_shorter_than: step equals threshold not blocked": {
			rule:            BlockedQuery{Pattern: "up", StepSizeShorterThan: model.Duration(time.Minute)},
			input:           BlockedQueryInput{Query: "up", StepKnown: true, Step: time.Minute},
			expectedMatches: false,
		},
		"step_size_shorter_than: step above threshold not blocked": {
			rule:            BlockedQuery{Pattern: "up", StepSizeShorterThan: model.Duration(time.Minute)},
			input:           BlockedQueryInput{Query: "up", StepKnown: true, Step: 5 * time.Minute},
			expectedMatches: false,
		},
		"step_size_shorter_than: step unknown not blocked": {
			rule:            BlockedQuery{Pattern: "up", StepSizeShorterThan: model.Duration(time.Minute)},
			input:           BlockedQueryInput{Query: "up", StepKnown: false},
			expectedMatches: false,
		},
		"step_size_shorter_than: zero disables filter": {
			rule:            BlockedQuery{Pattern: "up", StepSizeShorterThan: 0},
			input:           BlockedQueryInput{Query: "up", StepKnown: true, Step: 30 * time.Second},
			expectedMatches: true,
		},

		// Combined conditions
		"all conditions met": {
			rule: BlockedQuery{
				Pattern:             ".*expensive.*",
				Regex:               true,
				TimeRangeLongerThan: model.Duration(24 * time.Hour),
				StepSizeShorterThan: model.Duration(time.Minute),
			},
			input: BlockedQueryInput{
				Query:         "rate(expensive_metric[5m])",
				QueryDuration: 48 * time.Hour,
				StepKnown:     true,
				Step:          30 * time.Second,
			},
			expectedMatches: true,
		},
		"all conditions met except time range": {
			rule: BlockedQuery{
				Pattern:             ".*expensive.*",
				Regex:               true,
				TimeRangeLongerThan: model.Duration(24 * time.Hour),
				StepSizeShorterThan: model.Duration(time.Minute),
			},
			input: BlockedQueryInput{
				Query:         "rate(expensive_metric[5m])",
				QueryDuration: 12 * time.Hour,
				StepKnown:     true,
				Step:          30 * time.Second,
			},
			expectedMatches: false,
		},
		"all conditions met except step": {
			rule: BlockedQuery{
				Pattern:             ".*expensive.*",
				Regex:               true,
				TimeRangeLongerThan: model.Duration(24 * time.Hour),
				StepSizeShorterThan: model.Duration(time.Minute),
			},
			input: BlockedQueryInput{
				Query:         "rate(expensive_metric[5m])",
				QueryDuration: 48 * time.Hour,
				StepKnown:     true,
				Step:          5 * time.Minute,
			},
			expectedMatches: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expectedMatches, tc.rule.MatchesRule(tc.input))
		})
	}
}
