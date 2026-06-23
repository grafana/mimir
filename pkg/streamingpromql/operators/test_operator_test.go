// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestTestOperator_MatchesHandlesAbsentLabels(t *testing.T) {
	// Verify that TestOperator.matches evaluates matchers against absent labels
	// the same way real TSDB does: an absent label has value "".

	op := &TestOperator{}

	// A series WITHOUT the "service" label.
	seriesWithoutService := labels.FromStrings("env", "prod", "node", "host-1")

	// Matcher that does NOT match empty string: service=~"checkout|payments"
	matcherNoEmpty, err := labels.NewMatcher(labels.MatchRegexp, "service", "checkout|payments")
	require.NoError(t, err)

	// Matcher that DOES match empty string: service=~"|checkout|payments"
	matcherWithEmpty, err := labels.NewMatcher(labels.MatchRegexp, "service", "|checkout|payments")
	require.NoError(t, err)

	// Absent label = "". "checkout|payments" does not match "" → false.
	result := op.matches(seriesWithoutService, []*labels.Matcher{matcherNoEmpty})
	require.False(t, result, "matcher without empty alternative should reject series with absent label")

	// Absent label = "". "|checkout|payments" matches "" → true.
	result = op.matches(seriesWithoutService, []*labels.Matcher{matcherWithEmpty})
	require.True(t, result, "matcher with empty alternative should accept series with absent label")

	// A series WITH the "service" label — matching value.
	seriesWithService := labels.FromStrings("env", "prod", "service", "checkout")

	result = op.matches(seriesWithService, []*labels.Matcher{matcherNoEmpty})
	require.True(t, result, "matcher should accept present label with matching value")

	result = op.matches(seriesWithService, []*labels.Matcher{matcherWithEmpty})
	require.True(t, result, "matcher with empty should accept present label with matching value")

	// A series WITH the "service" label — non-matching value.
	seriesWithOtherService := labels.FromStrings("env", "prod", "service", "unknown")

	result = op.matches(seriesWithOtherService, []*labels.Matcher{matcherNoEmpty})
	require.False(t, result, "matcher should reject present label with non-matching value")

	result = op.matches(seriesWithOtherService, []*labels.Matcher{matcherWithEmpty})
	require.False(t, result, "matcher with empty should reject present label with non-matching value")
}
