// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/activitytracker"
)

func TestActivityTrackingMiddleware_DeleteAfterInnerHandler(t *testing.T) {
	activityFile := filepath.Join(t.TempDir(), "activity-tracker")
	reg := prometheus.NewPedanticRegistry()

	at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, at.Close()) })

	// The inner handler checks that the activity is present while it runs and also defers a write after ServeHTTP
	// returns, simulating what gzip's defer gw.Close() does in production.
	var deferredWriteDone bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
		assert.NoError(t, err)
		assert.Len(t, activities, 1)

		// Simulate work that happens after ServeHTTP returns (e.g. gzip Close).
		w.WriteHeader(http.StatusOK)
		deferredWriteDone = true
	})

	handler := NewActivityTrackingMiddleware(at, log.NewNopLogger(), inner)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up", nil)
	req.Header.Set("X-Scope-OrgID", "tenant1")
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	assert.True(t, deferredWriteDone)

	// After ServeHTTP returns the activity must be deleted.
	activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
	require.NoError(t, err)
	require.Empty(t, activities)
}

func TestActivityTrackingMiddleware_TenantIDFromHeader(t *testing.T) {
	activityFile := filepath.Join(t.TempDir(), "activity-tracker")
	reg := prometheus.NewPedanticRegistry()

	at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, at.Close()) })

	var capturedActivity string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
		assert.NoError(t, err)
		if assert.Len(t, activities, 1) {
			capturedActivity = activities[0].Activity
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := NewActivityTrackingMiddleware(at, log.NewNopLogger(), inner)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up", nil)
	// Set the org ID header directly — auth middleware has not run yet at this layer.
	req.Header.Set("X-Scope-OrgID", "my-tenant")
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	assert.Contains(t, capturedActivity, "user:my-tenant")
}

func TestActivityTrackingMiddleware_ParamParsing(t *testing.T) {
	activityFile := filepath.Join(t.TempDir(), "activity-tracker")
	reg := prometheus.NewPedanticRegistry()

	at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, at.Close()) })

	for _, tc := range []struct {
		name           string
		method         string
		url            string
		body           string
		contentType    string
		expectedParams string
	}{
		{
			name:           "GET with query params",
			method:         http.MethodGet,
			url:            "/api/v1/query?query=up&time=42",
			expectedParams: "query=up&time=42",
		},
		{
			name:           "POST form-encoded",
			method:         http.MethodPost,
			url:            "/api/v1/query",
			body:           "query=up&time=42",
			contentType:    "application/x-www-form-urlencoded",
			expectedParams: "query=up&time=42",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var capturedActivity string
			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
				assert.NoError(t, err)
				if assert.Len(t, activities, 1) {
					capturedActivity = activities[0].Activity
				}
				// Verify inner handler can still parse form independently.
				if tc.method == http.MethodPost {
					assert.NoError(t, r.ParseForm())
					assert.Equal(t, "up", r.FormValue("query"))
				}
				w.WriteHeader(http.StatusOK)
			})

			handler := NewActivityTrackingMiddleware(at, log.NewNopLogger(), inner)

			var bodyReader io.Reader
			if tc.body != "" {
				bodyReader = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.url, bodyReader)
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			req.Header.Set("X-Scope-OrgID", "tenant1")
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)

			assert.Contains(t, capturedActivity, tc.expectedParams)
		})
	}
}
