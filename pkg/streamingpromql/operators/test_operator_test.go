// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestTestOperator_MatchesHandlesAbsentLabels(t *testing.T) {
	// This test documents a known deficiency in TestOperator.matches:
	// it does not evaluate matchers against absent labels. A matcher
	// like foo=~"bar" (which does NOT match empty string) should reject
	// a series without the "foo" label, but TestOperator.matches returns
	// true because it only iterates over labels that exist on the series.
	//
	// This means unit tests using TestOperator are more permissive than
	// real storage (TSDB / store-gateway) when matchers target labels
	// that are absent from some series.

	op := &TestOperator{}

	// A series WITHOUT the "service" label.
	seriesWithoutService := labels.FromStrings("entity_type", "Node", "env", "prod")

	// Matcher that does NOT match empty string: service=~"checkout|payments"
	matcherNoEmpty, err := labels.NewMatcher(labels.MatchRegexp, "service", "checkout|payments")
	require.NoError(t, err)

	// Matcher that DOES match empty string: service=~"|checkout|payments"
	matcherWithEmpty, err := labels.NewMatcher(labels.MatchRegexp, "service", "|checkout|payments")
	require.NoError(t, err)

	// BUG: TestOperator.matches returns true for BOTH matchers when the label is absent.
	// The correct behavior (matching real TSDB) would be:
	//   matcherNoEmpty  -> false (absent label = "", which doesn't match "checkout|payments")
	//   matcherWithEmpty -> true  (absent label = "", which matches "|checkout|payments")
	//
	// But TestOperator.matches returns true for both because it never evaluates
	// the matcher when the label is absent from the series.
	result := op.matches(seriesWithoutService, []*labels.Matcher{matcherNoEmpty})
	// This documents the current (incorrect) behavior:
	require.True(t, result, "TestOperator.matches incorrectly returns true for absent label with non-empty-matching regex - this is a known bug")

	// This is the correct behavior for both TestOperator AND real storage:
	result = op.matches(seriesWithoutService, []*labels.Matcher{matcherWithEmpty})
	require.True(t, result, "matcher with empty alternative should match absent label")

	// A series WITH the "service" label should work correctly in all cases:
	seriesWithService := labels.FromStrings("entity_type", "Service", "env", "prod", "service", "checkout")

	result = op.matches(seriesWithService, []*labels.Matcher{matcherNoEmpty})
	require.True(t, result, "matcher without empty should match present label with matching value")

	result = op.matches(seriesWithService, []*labels.Matcher{matcherWithEmpty})
	require.True(t, result, "matcher with empty should match present label with matching value")

	// Non-matching present label should be rejected by both:
	seriesWithOtherService := labels.FromStrings("entity_type", "Service", "env", "prod", "service", "unknown")

	result = op.matches(seriesWithOtherService, []*labels.Matcher{matcherNoEmpty})
	require.False(t, result, "matcher should reject present label with non-matching value")

	result = op.matches(seriesWithOtherService, []*labels.Matcher{matcherWithEmpty})
	require.False(t, result, "matcher with empty should reject present label with non-matching value")
}
