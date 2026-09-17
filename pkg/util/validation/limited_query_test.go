// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestLimitedQuery_IsExpired(t *testing.T) {
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
			q := LimitedQuery{ExpiresAt: tc.expiresAt}
			require.Equal(t, tc.expected, q.IsExpired(now))
		})
	}
}

func TestLimitedQuery_MetadataFieldsRoundTripThroughYAML(t *testing.T) {
	input := LimitedQuery{
		Query:            "rate(metric_counter[5m])",
		AllowedFrequency: time.Minute,
		Reason:           "the query is expensive and should not run more than once a minute",
		ID:               "limit-metric-counter-rate",
		Note:             "added per incident INC-1234",
		CreatedBy:        "alice",
		CreatedAt:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:        time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
	}

	data, err := yaml.Marshal(input)
	require.NoError(t, err)

	var output LimitedQuery
	require.NoError(t, yaml.Unmarshal(data, &output))
	require.Equal(t, input, output)
}

func TestLimitedQuery_UnmarshalYAML_MissingMetadataFields(t *testing.T) {
	const in = `
query: rate(metric_counter[5m])
allowed_frequency: 1m
`
	var output LimitedQuery
	require.NoError(t, yaml.Unmarshal([]byte(in), &output))
	require.Equal(t, LimitedQuery{
		Query:            "rate(metric_counter[5m])",
		AllowedFrequency: time.Minute,
	}, output)
}
