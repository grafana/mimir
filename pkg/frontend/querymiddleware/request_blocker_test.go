// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestRequestBlocker_IsBlocked(t *testing.T) {
	const userID = "user-1"
	// Mock the limits.
	limits := multiTenantMockLimits{
		byTenant: map[string]mockLimits{
			userID: {
				blockedRequests: []*validation.BlockedRequest{
					{Path: "/blocked-by-path"},
					{Method: "POST"},
					{QueryParams: map[string]string{"foo": "bar"}},
					{Path: "/blocked-by-path2", Method: "GET", QueryParams: map[string]string{"foo": "bar2"}},
				},
			},
		},
	}

	tests := []struct {
		name     string
		request  func() *http.Request
		expected error
	}{
		{
			name: "request is not blocked",
			request: func() *http.Request {
				req, err := http.NewRequest(http.MethodGet, "/not-blocked", nil)
				require.NoError(t, err)
				return req
			},
			expected: nil,
		},
		{
			name: "request is blocked by path",
			request: func() *http.Request {
				req, err := http.NewRequest(http.MethodGet, "/blocked-by-path", nil)
				require.NoError(t, err)
				return req
			},
			expected: newRequestBlockedError(),
		},
		{
			name: "request is blocked by method",
			request: func() *http.Request {
				req, err := http.NewRequest(http.MethodPost, "/not-blocked", nil)
				require.NoError(t, err)
				return req
			},
			expected: newRequestBlockedError(),
		},
		{
			name: "request is blocked by query params",
			request: func() *http.Request {
				req, err := http.NewRequest(http.MethodGet, "/not-blocked?foo=bar", nil)
				require.NoError(t, err)
				return req
			},
			expected: newRequestBlockedError(),
		},
		{
			name: "request is blocked by path, method and query params",
			request: func() *http.Request {
				req, err := http.NewRequest(http.MethodGet, "/blocked-by-path2?foo=bar2", nil)
				require.NoError(t, err)
				return req
			},
			expected: newRequestBlockedError(),
		},
		{
			name: "request does not fully match blocked request (different method)",
			request: func() *http.Request {
				req, err := http.NewRequest(http.MethodDelete, "/blocked-by-path2?foo=bar2", nil)
				require.NoError(t, err)
				return req
			},
			expected: nil,
		},
		{
			name: "request does not fully match blocked request (different query params)",
			request: func() *http.Request {
				req, err := http.NewRequest(http.MethodGet, "/blocked-by-path2?foo=bar3", nil)
				require.NoError(t, err)
				return req
			},
			expected: nil,
		},
		{
			name: "request does not fully match blocked request (different path)",
			request: func() *http.Request {
				req, err := http.NewRequest(http.MethodGet, "/blocked-by-path3?foo=bar2", nil)
				require.NoError(t, err)
				return req
			},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blocker := newRequestBlocker(limits, log.NewNopLogger(), prometheus.NewRegistry())

			req := tt.request()
			ctx := user.InjectOrgID(context.Background(), userID)
			req = req.WithContext(ctx)

			err := blocker.isBlocked(req)
			assert.Equal(t, err, tt.expected)
		})
	}
}
