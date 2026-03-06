// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	shared "github.com/grafana/mimir/pkg/labelaccess"
	"github.com/grafana/mimir/pkg/util/propagation"
)

// singlePolicySet returns a LabelPolicySet with one policy for the given tenant.
func singlePolicySet(tenantID, name, value string) shared.LabelPolicySet {
	return shared.LabelPolicySet{
		tenantID: []*shared.LabelPolicy{
			{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, name, value)}},
		},
	}
}

func TestWrapTripperware_InjectsPolicyHash(t *testing.T) {
	var capturedCtx context.Context

	inner := querymiddleware.Tripperware(func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			capturedCtx = r.Context()
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})
	})

	trw := WrapTripperware(inner)
	rt := trw(http.DefaultTransport)

	policySet := singlePolicySet("tenant-a", "env", "prod")
	ctx := shared.InjectLabelMatchersContext(context.Background(), policySet)
	req := httptest.NewRequest(http.MethodGet, "/query", nil).WithContext(ctx)

	_, err := rt.RoundTrip(req)
	require.NoError(t, err)

	hash, ok := capturedCtx.Value(contextKeyLabelPolicyHash).(string)
	require.True(t, ok, "expected policy hash in context")
	assert.Equal(t, policySet.Hash(), hash)
}

func TestWrapTripperware_NoPoliciesNoHash(t *testing.T) {
	var capturedCtx context.Context

	inner := querymiddleware.Tripperware(func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			capturedCtx = r.Context()
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})
	})

	trw := WrapTripperware(inner)
	rt := trw(http.DefaultTransport)

	// Inject an empty policy set — no policies, so no hash should be added.
	ctx := shared.InjectLabelMatchersContext(context.Background(), shared.LabelPolicySet{})
	req := httptest.NewRequest(http.MethodGet, "/query", nil).WithContext(ctx)

	_, err := rt.RoundTrip(req)
	require.NoError(t, err)

	_, ok := capturedCtx.Value(contextKeyLabelPolicyHash).(string)
	assert.False(t, ok, "expected no policy hash in context when policy set is empty")
}

type stubCacheKeyGenerator struct {
	queryRequestKey    string
	queryRequestLimKey string
	queryRequestErrKey string
	labelValuesKey     *querymiddleware.GenericQueryCacheKey
	labelValuesCardKey *querymiddleware.GenericQueryCacheKey
	labelValuesCardErr error
}

func (s stubCacheKeyGenerator) QueryRequest(_ context.Context, tenantID string, _ querymiddleware.MetricsQueryRequest) string {
	return tenantID + ":" + s.queryRequestKey
}
func (s stubCacheKeyGenerator) QueryRequestLimiter(_ context.Context, tenantID string, _ querymiddleware.MetricsQueryRequest) string {
	return tenantID + ":" + s.queryRequestLimKey
}
func (s stubCacheKeyGenerator) QueryRequestError(_ context.Context, tenantID string, _ querymiddleware.MetricsQueryRequest) string {
	return tenantID + ":" + s.queryRequestErrKey
}
func (s stubCacheKeyGenerator) LabelValues(_ *http.Request) (*querymiddleware.GenericQueryCacheKey, error) {
	return s.labelValuesKey, nil
}
func (s stubCacheKeyGenerator) LabelValuesCardinality(_ *http.Request) (*querymiddleware.GenericQueryCacheKey, error) {
	return s.labelValuesCardKey, s.labelValuesCardErr
}

func ctxWithPolicyHash(hash string) context.Context {
	return context.WithValue(context.Background(), contextKeyLabelPolicyHash, hash)
}

func TestCacheKeyGenerator_QueryRequest_WithPolicy(t *testing.T) {
	gen := NewCacheSplitter(stubCacheKeyGenerator{queryRequestKey: "key"})
	ctx := ctxWithPolicyHash("abc123")
	got := gen.QueryRequest(ctx, "tenant", nil)
	assert.Equal(t, "tenant!abc123:key", got)
}

func TestCacheKeyGenerator_QueryRequest_WithoutPolicy(t *testing.T) {
	gen := NewCacheSplitter(stubCacheKeyGenerator{queryRequestKey: "key"})
	got := gen.QueryRequest(context.Background(), "tenant", nil)
	assert.Equal(t, "tenant:key", got)
}

func TestCacheKeyGenerator_QueryRequestLimiter_WithPolicy(t *testing.T) {
	gen := NewCacheSplitter(stubCacheKeyGenerator{queryRequestLimKey: "lim"})
	ctx := ctxWithPolicyHash("abc123")
	got := gen.QueryRequestLimiter(ctx, "tenant", nil)
	assert.Equal(t, "tenant!abc123:lim", got)
}

func TestCacheKeyGenerator_QueryRequestError_WithPolicy(t *testing.T) {
	gen := NewCacheSplitter(stubCacheKeyGenerator{queryRequestErrKey: "err"})
	ctx := ctxWithPolicyHash("abc123")
	got := gen.QueryRequestError(ctx, "tenant", nil)
	assert.Equal(t, "tenant!abc123:err", got)
}

func TestCacheKeyGenerator_LabelValues_WithPolicy(t *testing.T) {
	delegate := stubCacheKeyGenerator{labelValuesKey: &querymiddleware.GenericQueryCacheKey{CacheKeyPrefix: "prefix"}}
	gen := NewCacheSplitter(delegate)

	req := httptest.NewRequest(http.MethodGet, "/label-values", nil)
	req = req.WithContext(ctxWithPolicyHash("abc123"))

	got, err := gen.LabelValues(req)
	require.NoError(t, err)
	assert.Equal(t, "prefix!abc123", got.CacheKeyPrefix)
}

func TestCacheKeyGenerator_LabelValues_WithoutPolicy(t *testing.T) {
	delegate := stubCacheKeyGenerator{labelValuesKey: &querymiddleware.GenericQueryCacheKey{CacheKeyPrefix: "prefix"}}
	gen := NewCacheSplitter(delegate)

	req := httptest.NewRequest(http.MethodGet, "/label-values", nil)

	got, err := gen.LabelValues(req)
	require.NoError(t, err)
	assert.Equal(t, "prefix", got.CacheKeyPrefix)
}

func TestCacheKeyGenerator_LabelValuesCardinality_WithPolicy(t *testing.T) {
	gen := NewCacheSplitter(stubCacheKeyGenerator{})

	req := httptest.NewRequest(http.MethodGet, "/cardinality/label_values", nil)
	req = req.WithContext(ctxWithPolicyHash("abc123"))

	got, err := gen.LabelValuesCardinality(req)
	require.Error(t, err)
	assert.ErrorIs(t, err, querymiddleware.ErrUnsupportedRequest)
	assert.Nil(t, got)
}

func TestCacheKeyGenerator_LabelValuesCardinality_WithoutPolicy(t *testing.T) {
	delegate := stubCacheKeyGenerator{labelValuesCardKey: &querymiddleware.GenericQueryCacheKey{CacheKeyPrefix: "card"}}
	gen := NewCacheSplitter(delegate)

	req := httptest.NewRequest(http.MethodGet, "/cardinality/label_values", nil)

	got, err := gen.LabelValuesCardinality(req)
	require.NoError(t, err)
	assert.Equal(t, "card", got.CacheKeyPrefix)
}

// --- NewInjector ---

func TestInjector_InjectsHeaders(t *testing.T) {
	policySet := singlePolicySet("tenant-a", "env", "prod")
	ctx := shared.InjectLabelMatchersContext(context.Background(), policySet)

	carrier := propagation.MapCarrier{}
	injector := NewInjector()
	err := injector.InjectToCarrier(ctx, carrier)
	require.NoError(t, err)

	values := carrier.GetAll(shared.HTTPHeaderKey)
	require.NotEmpty(t, values, "expected LBAC headers to be injected")
}

func TestInjector_NoPoliciesNoHeaders(t *testing.T) {
	ctx := shared.InjectLabelMatchersContext(context.Background(), shared.LabelPolicySet{})

	carrier := propagation.MapCarrier{}
	injector := NewInjector()
	err := injector.InjectToCarrier(ctx, carrier)
	require.NoError(t, err)

	values := carrier.GetAll(shared.HTTPHeaderKey)
	assert.Empty(t, values, "expected no LBAC headers when policy set is empty")
}

// roundTripperFunc is a helper to implement http.RoundTripper with a function.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
