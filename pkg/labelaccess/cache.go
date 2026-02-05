// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"fmt"
	"net/http"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
)

const (
	// labelPolicySeparator is used to separate the tenant ID from the label policy hash in cache keys.
	labelPolicySeparator string = "!"
)

// CacheKeyGenerator changes the userID for a cache key if the current request is using LBAC.
// It then delegates the actual key generation to another querymiddleware.CacheKeyGenerator.
type CacheKeyGenerator struct {
	delegate querymiddleware.CacheKeyGenerator
}

// NewCacheKeyGenerator creates a new CacheKeyGenerator that wraps the provided delegate.
func NewCacheKeyGenerator(delegate querymiddleware.CacheKeyGenerator) CacheKeyGenerator {
	return CacheKeyGenerator{
		delegate: delegate,
	}
}

// QueryRequest implements querymiddleware.CacheKeyGenerator.
func (c CacheKeyGenerator) QueryRequest(ctx context.Context, tenantID string, r querymiddleware.MetricsQueryRequest) string {
	if labelPolicyHash, _ := ctx.Value(contextKeyLabelPolicyHash).(string); len(labelPolicyHash) > 0 {
		tenantID = tenantID + labelPolicySeparator + labelPolicyHash
	}
	return c.delegate.QueryRequest(ctx, tenantID, r)
}

// QueryRequestLimiter implements querymiddleware.CacheKeyGenerator.
func (c CacheKeyGenerator) QueryRequestLimiter(ctx context.Context, tenantID string, r querymiddleware.MetricsQueryRequest) string {
	if labelPolicyHash, _ := ctx.Value(contextKeyLabelPolicyHash).(string); len(labelPolicyHash) > 0 {
		tenantID = tenantID + labelPolicySeparator + labelPolicyHash
	}
	return c.delegate.QueryRequestLimiter(ctx, tenantID, r)
}

// QueryRequestError implements querymiddleware.CacheKeyGenerator.
func (c CacheKeyGenerator) QueryRequestError(ctx context.Context, tenantID string, r querymiddleware.MetricsQueryRequest) string {
	if labelPolicyHash, _ := ctx.Value(contextKeyLabelPolicyHash).(string); len(labelPolicyHash) > 0 {
		tenantID = tenantID + labelPolicySeparator + labelPolicyHash
	}
	return c.delegate.QueryRequestError(ctx, tenantID, r)
}

// LabelValues implements querymiddleware.CacheKeyGenerator.
func (c CacheKeyGenerator) LabelValues(r *http.Request) (*querymiddleware.GenericQueryCacheKey, error) {
	cacheKey, err := c.delegate.LabelValues(r)
	if err != nil {
		return nil, err
	}
	if labelPolicyHash, _ := r.Context().Value(contextKeyLabelPolicyHash).(string); len(labelPolicyHash) > 0 {
		cacheKey.CacheKeyPrefix += labelPolicySeparator + labelPolicyHash
	}
	return cacheKey, nil
}

// LabelValuesCardinality implements querymiddleware.CacheKeyGenerator.
func (c CacheKeyGenerator) LabelValuesCardinality(r *http.Request) (*querymiddleware.GenericQueryCacheKey, error) {
	if labelPolicyHash, _ := r.Context().Value(contextKeyLabelPolicyHash).(string); len(labelPolicyHash) > 0 {
		return nil, fmt.Errorf("label values cardinality caching is not supported with LBAC: %w", querymiddleware.ErrUnsupportedRequest)
	}
	return c.delegate.LabelValuesCardinality(r)
}
