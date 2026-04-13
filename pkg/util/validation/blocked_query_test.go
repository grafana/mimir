// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
)

func TestBlockedQueriesConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		input    BlockedQueriesConfig
		expected error
	}{
		"no rules": {},
		"valid rule with pattern": {
			input: BlockedQueriesConfig{
				{Pattern: ".*", Regex: true},
			},
		},
		"rule with empty pattern": {
			input: BlockedQueriesConfig{
				{TimeRangeLongerThan: model.Duration(24 * time.Hour)},
			},
			expected: errors.New("blocked_queries[0]: pattern is required"),
		},
		"second rule with empty pattern": {
			input: BlockedQueriesConfig{
				{Pattern: ".*", Regex: true},
				{Pattern: "", TimeRangeLongerThan: model.Duration(24 * time.Hour)},
			},
			expected: errors.New("blocked_queries[1]: pattern is required"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.input.Validate())
		})
	}
}
