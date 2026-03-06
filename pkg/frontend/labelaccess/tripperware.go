// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/backend-enterprise/pkg/enterprise/mimir/labelaccess/tripperware.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Grafana Labs

package labelaccess

import (
	"context"
	"fmt"
	"net/http"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/util/propagation"

	shared "github.com/grafana/mimir/pkg/labelaccess"
)

type contextKey int

const (
	contextKeyLabelPolicyHash contextKey = iota

	labelPolicySeparator string = "!"
)

type tripperwareWrapper struct {
	next http.RoundTripper
}

// WrapTripperware takes the provided tripperware and injects the label policy hash in the context
// before passing the request to the wrapped tripperware.
func WrapTripperware(qrTrw querymiddleware.Tripperware) querymiddleware.Tripperware {
	return func(next http.RoundTripper) http.RoundTripper {
		// Call chain for RoundTrip() is:
		// tripperwareWrapper.RoundTrip -> querymiddleware -> next
		lbacTrw := &tripperwareWrapper{
			next: qrTrw(next),
		}
		return lbacTrw
	}
}

func (t *tripperwareWrapper) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx := r.Context()
	labelPolicySet, err := shared.ExtractLabelMatchersContext(ctx)
	// This should never happen anyways because we have auth middleware before this.
	if err != nil {
		return nil, err
	}

	// If the instance policies are set on the query, then inject the hash of the policy set,
	// so that CacheKeyGenerator can use it to cache results for the tenant differently with and without the policy.
	if len(labelPolicySet) != 0 {
		ctx = context.WithValue(ctx, contextKeyLabelPolicyHash, labelPolicySet.Hash())
		r = r.Clone(ctx)
	}

	return t.next.RoundTrip(r)
}

// NewInjector returns a propagation.Injector that re-injects LBAC headers into the
// request carrier after querymiddleware processing (which strips unknown headers).
func NewInjector() propagation.Injector {
	return &injector{}
}

type injector struct{}

func (i *injector) InjectToCarrier(ctx context.Context, carrier propagation.Carrier) error {
	labelPolicySet, err := shared.ExtractLabelMatchersContext(ctx)
	// This should never happen anyways because we have auth middleware before this.
	if err != nil {
		return err
	}

	// If the instance policies are set on the query, ensure the LBAC configs are reinjected into the HTTP request
	// as headers. This is necessary that this is done after the querymiddleware has done its job,
	// because the RoundTrippers in querymiddleware throw away any HTTP headers they don't care about.
	if len(labelPolicySet) != 0 {
		matchers, err := shared.InjectLabelMatchersSlice(labelPolicySet)
		if err != nil {
			return err
		}

		carrier.SetAll(shared.HTTPHeaderKey, matchers)
	}

	return nil
}

// CacheKeyGenerator changes the userID for a cache key if the current request is using LBAC.
// It then delegates the actual key generation to another querymiddleware.CacheKeyGenerator.
type CacheKeyGenerator struct {
	delegate querymiddleware.CacheKeyGenerator
}

// NewCacheSplitter wraps the given CacheKeyGenerator to incorporate the LBAC policy hash
// into all cache keys, ensuring different policies never share cached results.
func NewCacheSplitter(delegate querymiddleware.CacheKeyGenerator) CacheKeyGenerator {
	return CacheKeyGenerator{
		delegate: delegate,
	}
}

// QueryRequest implements querymiddleware.CacheKeyGenerator
func (c CacheKeyGenerator) QueryRequest(ctx context.Context, tenantID string, r querymiddleware.MetricsQueryRequest) string {
	if labelPolicyHash, _ := ctx.Value(contextKeyLabelPolicyHash).(string); len(labelPolicyHash) > 0 {
		tenantID = tenantID + labelPolicySeparator + labelPolicyHash
	}
	return c.delegate.QueryRequest(ctx, tenantID, r)
}

// QueryRequestLimiter implements querymiddleware.CacheKeyGenerator
func (c CacheKeyGenerator) QueryRequestLimiter(ctx context.Context, tenantID string, r querymiddleware.MetricsQueryRequest) string {
	if labelPolicyHash, _ := ctx.Value(contextKeyLabelPolicyHash).(string); len(labelPolicyHash) > 0 {
		tenantID = tenantID + labelPolicySeparator + labelPolicyHash
	}
	return c.delegate.QueryRequestLimiter(ctx, tenantID, r)
}

// QueryRequestError implements querymiddleware.CacheKeyGenerator
func (c CacheKeyGenerator) QueryRequestError(ctx context.Context, tenantID string, r querymiddleware.MetricsQueryRequest) string {
	if labelPolicyHash, _ := ctx.Value(contextKeyLabelPolicyHash).(string); len(labelPolicyHash) > 0 {
		tenantID = tenantID + labelPolicySeparator + labelPolicyHash
	}
	return c.delegate.QueryRequestError(ctx, tenantID, r)
}

// LabelValues implements querymiddleware.CacheKeyGenerator
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

// LabelValuesCardinality implements querymiddleware.CacheKeyGenerator
func (c CacheKeyGenerator) LabelValuesCardinality(r *http.Request) (*querymiddleware.GenericQueryCacheKey, error) {
	if labelPolicyHash, _ := r.Context().Value(contextKeyLabelPolicyHash).(string); len(labelPolicyHash) > 0 {
		return nil, fmt.Errorf("label values cardinality caching is not supported with LBAC: %w", querymiddleware.ErrUnsupportedRequest)
	}
	return c.delegate.LabelValuesCardinality(r)
}
