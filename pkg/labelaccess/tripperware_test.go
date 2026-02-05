// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRoundTripper struct {
	receivedRequest *http.Request
}

func (m *mockRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	m.receivedRequest = r
	return &http.Response{StatusCode: 200}, nil
}

func TestWrapTripperware(t *testing.T) {
	t.Run("no policies passes through without modification", func(t *testing.T) {
		ctx := context.Background()
		ctx = user.InjectOrgID(ctx, "tenant-1")
		ctx = InjectLabelMatchersContext(ctx, LabelPolicySet{})

		mockNext := &mockRoundTripper{}
		mockTripperware := func(next http.RoundTripper) http.RoundTripper {
			return mockNext
		}

		wrapped := WrapTripperware(mockTripperware)
		rt := wrapped(http.DefaultTransport)

		req := httptest.NewRequest("GET", "/query", nil)
		req = req.WithContext(ctx)

		_, _ = rt.RoundTrip(req)

		// Verify hash context key is not present
		hash, ok := mockNext.receivedRequest.Context().Value(contextKeyLabelPolicyHash).(string)
		assert.False(t, ok)
		assert.Empty(t, hash)
	})

	t.Run("with policies injects hash into context", func(t *testing.T) {
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

		mockNext := &mockRoundTripper{}
		mockTripperware := func(next http.RoundTripper) http.RoundTripper {
			return mockNext
		}

		wrapped := WrapTripperware(mockTripperware)
		rt := wrapped(http.DefaultTransport)

		req := httptest.NewRequest("GET", "/query", nil)
		req = req.WithContext(ctx)

		_, _ = rt.RoundTrip(req)

		// Verify hash is present in context
		hash, ok := mockNext.receivedRequest.Context().Value(contextKeyLabelPolicyHash).(string)
		require.True(t, ok)
		assert.NotEmpty(t, hash)

		// Hash should be the same as policySet.Hash()
		expectedHash := policySet.Hash()
		assert.Equal(t, expectedHash, hash)
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

		mockNext := &mockRoundTripper{}
		mockTripperware := func(next http.RoundTripper) http.RoundTripper {
			return mockNext
		}

		wrapped := WrapTripperware(mockTripperware)
		rt := wrapped(http.DefaultTransport)

		ctx1 := context.Background()
		ctx1 = user.InjectOrgID(ctx1, tenantID)
		ctx1 = InjectLabelMatchersContext(ctx1, policySet1)
		req1 := httptest.NewRequest("GET", "/query", nil)
		req1 = req1.WithContext(ctx1)
		_, _ = rt.RoundTrip(req1)
		hash1, ok := mockNext.receivedRequest.Context().Value(contextKeyLabelPolicyHash).(string)
		require.True(t, ok)

		ctx2 := context.Background()
		ctx2 = user.InjectOrgID(ctx2, tenantID)
		ctx2 = InjectLabelMatchersContext(ctx2, policySet2)
		req2 := httptest.NewRequest("GET", "/query", nil)
		req2 = req2.WithContext(ctx2)
		_, _ = rt.RoundTrip(req2)
		hash2, ok := mockNext.receivedRequest.Context().Value(contextKeyLabelPolicyHash).(string)
		require.True(t, ok)

		assert.NotEqual(t, hash1, hash2, "different policies should generate different hashes")
	})
}
