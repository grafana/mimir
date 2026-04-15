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
