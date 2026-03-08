// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"testing"

	"github.com/grafana/mimir/pkg/util/propagation"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractor(t *testing.T) {
	t.Run("extracts policies from carrier", func(t *testing.T) {
		policySet := LabelPolicySet{
			"tenant-1": []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
					},
				},
			},
		}

		carrier := propagation.MapCarrier{}
		for tenant, policies := range policySet {
			for _, policy := range policies {
				headerValue, err := policyToHeaderValue(tenant, policy)
				require.NoError(t, err)
				carrier[HTTPHeaderKey] = append(carrier[HTTPHeaderKey], headerValue)
			}
		}

		extractor := NewExtractor()
		ctx, err := extractor.ExtractFromCarrier(context.Background(), carrier)
		require.NoError(t, err)

		extracted, err := ExtractLabelMatchersContext(ctx)
		require.NoError(t, err)
		assert.Equal(t, policySet, extracted)
	})

	t.Run("empty carrier returns empty policy set", func(t *testing.T) {
		carrier := propagation.MapCarrier{}

		extractor := NewExtractor()
		ctx, err := extractor.ExtractFromCarrier(context.Background(), carrier)
		require.NoError(t, err)

		extracted, err := ExtractLabelMatchersContext(ctx)
		// When no policies exist, we get errNoMatcherSource
		if err != nil {
			assert.Equal(t, errNoMatcherSource, err)
			assert.Empty(t, extracted)
		} else {
			assert.Empty(t, extracted)
		}
	})

	t.Run("does not overwrite existing policies in context", func(t *testing.T) {
		existingPolicySet := LabelPolicySet{
			"tenant-1": []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "existing", "true"),
					},
				},
			},
		}

		ctx := InjectLabelMatchersContext(context.Background(), existingPolicySet)

		newPolicySet := LabelPolicySet{
			"tenant-2": []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "new", "true"),
					},
				},
			},
		}

		carrier := propagation.MapCarrier{}
		for tenant, policies := range newPolicySet {
			for _, policy := range policies {
				headerValue, err := policyToHeaderValue(tenant, policy)
				require.NoError(t, err)
				carrier[HTTPHeaderKey] = append(carrier[HTTPHeaderKey], headerValue)
			}
		}

		extractor := NewExtractor()
		ctx, err := extractor.ExtractFromCarrier(ctx, carrier)
		require.NoError(t, err)

		// Should still have the existing policy set, not the new one
		extracted, err := ExtractLabelMatchersContext(ctx)
		require.NoError(t, err)
		assert.Equal(t, existingPolicySet, extracted)
	})
}

func TestInjector(t *testing.T) {
	t.Run("injects policies into carrier", func(t *testing.T) {
		policySet := LabelPolicySet{
			"tenant-1": []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
					},
				},
			},
		}

		ctx := InjectLabelMatchersContext(context.Background(), policySet)

		carrier := propagation.MapCarrier{}
		injector := NewInjector()
		err := injector.InjectToCarrier(ctx, carrier)
		require.NoError(t, err)

		// Verify carrier contains the policy header
		values, ok := carrier[HTTPHeaderKey]
		require.True(t, ok)
		require.Len(t, values, 1)
		assert.Contains(t, values[0], "tenant-1:")
	})

	t.Run("no policies results in no injection", func(t *testing.T) {
		ctx := InjectLabelMatchersContext(context.Background(), LabelPolicySet{})

		carrier := propagation.MapCarrier{}
		injector := NewInjector()
		err := injector.InjectToCarrier(ctx, carrier)
		require.NoError(t, err)

		// Carrier should be empty
		values, ok := carrier[HTTPHeaderKey]
		assert.False(t, ok)
		assert.Empty(t, values)
	})

	t.Run("no policies in context results in no injection", func(t *testing.T) {
		ctx := context.Background()

		carrier := propagation.MapCarrier{}
		injector := NewInjector()
		err := injector.InjectToCarrier(ctx, carrier)
		require.NoError(t, err)

		// Carrier should be empty
		values, ok := carrier[HTTPHeaderKey]
		assert.False(t, ok)
		assert.Empty(t, values)
	})
}

func TestPropagationRoundtrip(t *testing.T) {
	t.Run("roundtrip preserves policy set", func(t *testing.T) {
		originalPolicySet := LabelPolicySet{
			"tenant-1": []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
						labels.MustNewMatcher(labels.MatchNotEqual, "debug", "true"),
					},
				},
			},
			"tenant-2": []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, "region", "us-.*"),
					},
				},
			},
		}

		// Inject into context
		ctx := InjectLabelMatchersContext(context.Background(), originalPolicySet)

		// Inject into carrier
		carrier := propagation.MapCarrier{}
		injector := NewInjector()
		err := injector.InjectToCarrier(ctx, carrier)
		require.NoError(t, err)

		// Extract from carrier
		extractor := NewExtractor()
		newCtx, err := extractor.ExtractFromCarrier(context.Background(), carrier)
		require.NoError(t, err)

		// Verify policy set is preserved
		extractedPolicySet, err := ExtractLabelMatchersContext(newCtx)
		require.NoError(t, err)

		// Compare using string representation since pointers will be different
		require.Len(t, extractedPolicySet, len(originalPolicySet))
		for tenant, originalPolicies := range originalPolicySet {
			extractedPolicies, ok := extractedPolicySet[tenant]
			require.True(t, ok, "tenant %s not found in extracted policy set", tenant)
			require.Len(t, extractedPolicies, len(originalPolicies))

			for i, originalPolicy := range originalPolicies {
				require.Len(t, extractedPolicies[i].Selector, len(originalPolicy.Selector))
				for j, originalMatcher := range originalPolicy.Selector {
					assert.Equal(t, originalMatcher.String(), extractedPolicies[i].Selector[j].String())
				}
			}
		}
	})
}
