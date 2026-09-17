// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
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
		"metadata fields set": {
			input: BlockedQueriesConfig{
				{
					Pattern:   "rate(metric_counter[5m])",
					ID:        "block-metric-counter-rate",
					Note:      "added per incident INC-1234",
					CreatedBy: "alice",
					CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
					ExpiresAt: time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
				},
			},
			expectedErrMsg: "", // none
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

func TestBlockedQuery_IsExpired(t *testing.T) {
	now := time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)

	tests := map[string]struct {
		expiresAt time.Time
		expected  bool
	}{
		"zero value never expires":              {expiresAt: time.Time{}, expected: false},
		"expiry in the future":                  {expiresAt: now.Add(time.Hour), expected: false},
		"expiry in the past":                    {expiresAt: now.Add(-time.Hour), expected: true},
		"expiry exactly now is not yet expired": {expiresAt: now, expected: false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			q := BlockedQuery{ExpiresAt: tc.expiresAt}
			assert.Equal(t, tc.expected, q.IsExpired(now))
		})
	}
}

func TestBlockedQuery_MetadataFieldsRoundTripThroughYAML(t *testing.T) {
	input := BlockedQuery{
		Pattern:   "rate(metric_counter[5m])",
		Reason:    "because the query is misconfigured",
		ID:        "block-metric-counter-rate",
		Note:      "added per incident INC-1234",
		CreatedBy: "alice",
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt: time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
	}

	data, err := yaml.Marshal(input)
	require.NoError(t, err)

	var output BlockedQuery
	require.NoError(t, yaml.Unmarshal(data, &output))
	require.Equal(t, input, output)
}

func TestBlockedQuery_UnmarshalYAML_MissingMetadataFields(t *testing.T) {
	const in = `
pattern: rate(metric_counter[5m])
reason: because the query is misconfigured
`
	var output BlockedQuery
	require.NoError(t, yaml.Unmarshal([]byte(in), &output))
	require.Equal(t, BlockedQuery{
		Pattern: "rate(metric_counter[5m])",
		Reason:  "because the query is misconfigured",
	}, output)
}
