// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubquerySpinOffDisabledQueriesConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		config      SubquerySpinOffDisabledQueriesConfig
		expectedErr string
	}{
		"empty config is valid": {
			config: nil,
		},
		"literal pattern is valid": {
			config: SubquerySpinOffDisabledQueriesConfig{{Pattern: "rate(metric_counter[5m])"}},
		},
		"valid regex pattern is valid": {
			config: SubquerySpinOffDisabledQueriesConfig{{Pattern: ".*expensive.*", Regex: true}},
		},
		"missing pattern is rejected": {
			config:      SubquerySpinOffDisabledQueriesConfig{{Pattern: "  "}},
			expectedErr: "subquery_spin_off_disabled_queries[0]: pattern is required",
		},
		"invalid regex pattern is rejected": {
			config:      SubquerySpinOffDisabledQueriesConfig{{Pattern: "[invalid", Regex: true}},
			expectedErr: `subquery_spin_off_disabled_queries[0]: invalid regex pattern "[invalid"`,
		},
		"invalid regex is ignored when regex is false": {
			config: SubquerySpinOffDisabledQueriesConfig{{Pattern: "[invalid", Regex: false}},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedErr)
			}
		})
	}
}
