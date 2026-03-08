// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCacheKeyGenerator struct {
	queryRequestKey             string
	queryRequestLimiterKey      string
	labelValuesKey              *querymiddleware.GenericQueryCacheKey
	labelValuesCardinalityError error
}

func (m *mockCacheKeyGenerator) QueryRequest(ctx context.Context, tenantID string, r querymiddleware.MetricsQueryRequest) string {
	// Return the key with the tenant ID (which may have been modified by the wrapper)
	return tenantID + ":" + m.queryRequestKey
}

func (m *mockCacheKeyGenerator) QueryRequestLimiter(ctx context.Context, tenantID string, r querymiddleware.MetricsQueryRequest) string {
	return tenantID + ":" + m.queryRequestLimiterKey
}

func (m *mockCacheKeyGenerator) QueryRequestError(ctx context.Context, tenantID string, r querymiddleware.MetricsQueryRequest) string {
	return ""
}

func (m *mockCacheKeyGenerator) LabelValues(r *http.Request) (*querymiddleware.GenericQueryCacheKey, error) {
	return m.labelValuesKey, nil
}

func (m *mockCacheKeyGenerator) LabelValuesCardinality(r *http.Request) (*querymiddleware.GenericQueryCacheKey, error) {
	return nil, m.labelValuesCardinalityError
}

func TestCacheKeyGenerator_QueryRequest(t *testing.T) {
	t.Run("no policies returns base cache key", func(t *testing.T) {
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "tenant-1")
		ctx = InjectLabelMatchersContext(ctx, LabelPolicySet{})

		mockGen := &mockCacheKeyGenerator{queryRequestKey: "base-key"}
		gen := NewCacheKeyGenerator(mockGen)

		key := gen.QueryRequest(ctx, "tenant-1", nil)
		assert.Equal(t, "tenant-1:base-key", key)
	})

	t.Run("single policy appends hash to cache key", func(t *testing.T) {
		const tenantID = "tenant-1"
		policySet := LabelPolicySet{
			tenantID: []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
					},
				},
			},
		}

		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, tenantID)
		ctx = InjectLabelMatchersContext(ctx, policySet)
		ctx = context.WithValue(ctx, contextKeyLabelPolicyHash, policySet.Hash())

		mockGen := &mockCacheKeyGenerator{queryRequestKey: "base-key"}
		gen := NewCacheKeyGenerator(mockGen)

		key := gen.QueryRequest(ctx, tenantID, nil)

		// Should append hash with ! separator
		assert.Contains(t, key, "tenant-1!")
		assert.Contains(t, key, "base-key")

		// Hash should be consistent
		key2 := gen.QueryRequest(ctx, tenantID, nil)
		assert.Equal(t, key, key2)
	})

	t.Run("multiple policies same hash", func(t *testing.T) {
		const tenantID = "tenant-1"
		policySet := LabelPolicySet{
			tenantID: []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
					},
				},
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "region", "us"),
					},
				},
			},
		}

		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, tenantID)
		ctx = InjectLabelMatchersContext(ctx, policySet)
		ctx = context.WithValue(ctx, contextKeyLabelPolicyHash, policySet.Hash())

		mockGen := &mockCacheKeyGenerator{queryRequestKey: "tenant-1:base-key"}
		gen := NewCacheKeyGenerator(mockGen)

		key := gen.QueryRequest(ctx, tenantID, nil)
		assert.Contains(t, key, "tenant-1!")
	})

	t.Run("different policies generate different hashes", func(t *testing.T) {
		const tenantID = "tenant-1"

		policySet1 := LabelPolicySet{
			tenantID: []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
					},
				},
			},
		}

		policySet2 := LabelPolicySet{
			tenantID: []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "env", "dev"),
					},
				},
			},
		}

		mockGen := &mockCacheKeyGenerator{queryRequestKey: "tenant-1:base-key"}
		gen := NewCacheKeyGenerator(mockGen)

		ctx1 := context.Background()
		ctx1 = user.InjectOrgID(ctx1, tenantID)
		ctx1 = InjectLabelMatchersContext(ctx1, policySet1)
		ctx1 = context.WithValue(ctx1, contextKeyLabelPolicyHash, policySet1.Hash())
		key1 := gen.QueryRequest(ctx1, tenantID, nil)

		ctx2 := context.Background()
		ctx2 = user.InjectOrgID(ctx2, tenantID)
		ctx2 = InjectLabelMatchersContext(ctx2, policySet2)
		ctx2 = context.WithValue(ctx2, contextKeyLabelPolicyHash, policySet2.Hash())
		key2 := gen.QueryRequest(ctx2, tenantID, nil)

		assert.NotEqual(t, key1, key2, "different policies should generate different cache keys")
	})
}

func TestCacheKeyGenerator_LabelValues(t *testing.T) {
	t.Run("no policies returns base cache key", func(t *testing.T) {
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "tenant-1")
		ctx = InjectLabelMatchersContext(ctx, LabelPolicySet{})

		mockGen := &mockCacheKeyGenerator{
			labelValuesKey: &querymiddleware.GenericQueryCacheKey{CacheKeyPrefix: "label-values-key"},
		}
		gen := NewCacheKeyGenerator(mockGen)

		req := httptest.NewRequest("GET", "/label/test/values", nil)
		req = req.WithContext(ctx)

		key, err := gen.LabelValues(req)
		require.NoError(t, err)
		assert.Equal(t, "label-values-key", key.CacheKeyPrefix)
	})

	t.Run("with policies appends hash", func(t *testing.T) {
		const tenantID = "tenant-1"
		policySet := LabelPolicySet{
			tenantID: []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
					},
				},
			},
		}

		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, tenantID)
		ctx = InjectLabelMatchersContext(ctx, policySet)
		ctx = context.WithValue(ctx, contextKeyLabelPolicyHash, policySet.Hash())

		mockGen := &mockCacheKeyGenerator{
			labelValuesKey: &querymiddleware.GenericQueryCacheKey{CacheKeyPrefix: "label-values-key"},
		}
		gen := NewCacheKeyGenerator(mockGen)

		req := httptest.NewRequest("GET", "/label/test/values", nil)
		req = req.WithContext(ctx)

		key, err := gen.LabelValues(req)
		require.NoError(t, err)
		assert.Contains(t, key.CacheKeyPrefix, "!")
		assert.NotEqual(t, "label-values-key", key.CacheKeyPrefix)
	})
}

func TestCacheKeyGenerator_LabelValuesCardinality(t *testing.T) {
	t.Run("with policies returns error", func(t *testing.T) {
		const tenantID = "tenant-1"
		policySet := LabelPolicySet{
			tenantID: []*LabelPolicy{
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
					},
				},
			},
		}

		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, tenantID)
		ctx = InjectLabelMatchersContext(ctx, policySet)
		ctx = context.WithValue(ctx, contextKeyLabelPolicyHash, policySet.Hash())

		mockGen := &mockCacheKeyGenerator{}
		gen := NewCacheKeyGenerator(mockGen)

		req := httptest.NewRequest("GET", "/label/test/values", nil)
		req = req.WithContext(ctx)

		_, err := gen.LabelValuesCardinality(req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "label values cardinality caching is not supported with LBAC")
	})

	t.Run("without policies delegates to base generator", func(t *testing.T) {
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "tenant-1")
		ctx = InjectLabelMatchersContext(ctx, LabelPolicySet{})

		mockGen := &mockCacheKeyGenerator{}
		gen := NewCacheKeyGenerator(mockGen)

		req := httptest.NewRequest("GET", "/label/test/values", nil)
		req = req.WithContext(ctx)

		_, err := gen.LabelValuesCardinality(req)
		require.NoError(t, err)
	})
}

func TestCacheKeyGenerator_QueryRequestLimiter(t *testing.T) {
	t.Run("delegates to base generator", func(t *testing.T) {
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "tenant-1")

		mockGen := &mockCacheKeyGenerator{queryRequestLimiterKey: "limiter-key"}
		gen := NewCacheKeyGenerator(mockGen)

		key := gen.QueryRequestLimiter(ctx, "tenant-1", nil)
		assert.Equal(t, "tenant-1:limiter-key", key)
	})
}
