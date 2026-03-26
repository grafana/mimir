// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/activitytracker"
)

func TestActivityTrackingMiddleware_DeleteAfterInnerHandler(t *testing.T) {
	activityFile := filepath.Join(t.TempDir(), "activity-tracker")
	reg := prometheus.NewPedanticRegistry()

	at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, at.Close()) })

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up", nil)
	req.Header.Set("X-Scope-OrgID", "tenant1")

	var innerHandlerDone bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
		require.NoError(t, err)
		require.Len(t, activities, 1)
		require.Equal(t, toActivityTrackerString(req, "", map[string][]string{"query": {"up"}}), activities[0].Activity)

		w.WriteHeader(http.StatusOK)
		innerHandlerDone = true
	})

	// Activity tracker wraps our pretend handler
	handler := NewActivityTrackingMiddleware(at, log.NewNopLogger(), inner)

	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	require.True(t, innerHandlerDone)

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
		require.NoError(t, err)
		require.Len(t, activities, 1)
		capturedActivity = activities[0].Activity
		w.WriteHeader(http.StatusOK)
	})

	handler := NewActivityTrackingMiddleware(at, log.NewNopLogger(), inner)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query?query=up", nil)
	// Set the org ID header directly — auth middleware has not run yet at this layer.
	req.Header.Set("X-Scope-OrgID", "my-tenant")
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	require.Contains(t, capturedActivity, "user:my-tenant")
}

func TestActivityTrackingMiddleware_ParamParsing(t *testing.T) {
	activityFile := filepath.Join(t.TempDir(), "activity-tracker")
	reg := prometheus.NewPedanticRegistry()

	at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, at.Close()) })

	for _, tc := range []struct {
		name               string
		method             string
		url                string
		body               string
		contentType        string
		expectedParams     string
		checkBodyPreserved bool
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
		{
			// Remote write sends a snappy-compressed protobuf body with Content-Type application/x-protobuf.
			// The middleware must not corrupt the binary body, and the handler must be able to read it as-is.
			// Since the body is not form-encoded, no query params are captured from the body.
			name:               "POST remote write (binary protobuf body is preserved for handler)",
			method:             http.MethodPost,
			url:                "/api/v1/push",
			body:               "\x00\x01\x02\x03binary-remote-write-payload",
			contentType:        "application/x-protobuf",
			expectedParams:     "(no params)",
			checkBodyPreserved: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var capturedActivity string
			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
				require.NoError(t, err)
				require.Len(t, activities, 1)
				capturedActivity = activities[0].Activity

				if tc.method == http.MethodPost {
					// Validate we can safely read the body and that it matches the original body.
					// The body has not been consumed by the activity tracker.
					got, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					require.Equal(t, tc.body, string(got))
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

			require.Contains(t, capturedActivity, tc.expectedParams)
		})
	}
}

// TestActivityTrackingMiddleware_NonFormEncodedRequests verifies that the middleware does not
// buffer the request body for non-form-encoded requests. Buffering would be harmful for
// large or streaming uploads (e.g. block file uploads, remote write) because it forces
// the entire body into memory before the handler can start.
func TestActivityTrackingMiddleware_NonFormEncodedRequests(t *testing.T) {
	activityFile := filepath.Join(t.TempDir(), "activity-tracker")
	reg := prometheus.NewPedanticRegistry()
	at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, at.Close()) })

	for _, tc := range []struct {
		name             string
		bodyContent      string
		contentType      string
		transferEncoding []string
	}{
		{
			// ContentLength=-1 with Transfer-Encoding: chunked means the body size is unknown
			// upfront. The middleware must not attempt to buffer it.
			name:             "chunked transfer encoding: body is passed through intact",
			bodyContent:      "chunked binary content",
			contentType:      "application/octet-stream",
			transferEncoding: []string{"chunked"},
		},
		{
			// Any content type that is not application/x-www-form-urlencoded must not cause
			// the middleware to buffer the body — the handler must receive the original bytes.
			name:        "non-standard content type: body is passed through intact",
			bodyContent: "binary payload \x00\x01\x02\x03 with non-standard content type",
			contentType: "application/vnd.custom-binary",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotBody string
			var capturedActivity string

			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
				require.NoError(t, err)
				require.Len(t, activities, 1)
				capturedActivity = activities[0].Activity
				got, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				gotBody = string(got)
				w.WriteHeader(http.StatusOK)
			})
			handler := NewActivityTrackingMiddleware(at, log.NewNopLogger(), inner)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/01ABC/files?path=chunks/000001", strings.NewReader(tc.bodyContent))
			req.Header.Set("Content-Type", tc.contentType)
			req.Header.Set("X-Scope-OrgID", "tenant1")
			if len(tc.transferEncoding) > 0 {
				req.ContentLength = -1
				req.TransferEncoding = tc.transferEncoding
			}
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)

			require.Equal(t, http.StatusOK, resp.Code)
			require.Equal(t, tc.bodyContent, gotBody)
			// URL query params are captured even though the body was not buffered.
			require.Equal(t, toActivityTrackerString(req, "", map[string][]string{"path": {"chunks/000001"}}), capturedActivity)
		})
	}
}

// TestActivityTrackingMiddleware_SlowUpload verifies that the middleware does not buffer
// the request body before calling the inner handler. It uses an io.Pipe to prove the
// handler starts before any body data arrives — if the middleware buffers the body it
// will block until the pipe is closed and the handler will never start.
func TestActivityTrackingMiddleware_SlowUpload(t *testing.T) {
	const bodyContent = "slow binary block file content"

	activityFile := filepath.Join(t.TempDir(), "activity-tracker")
	reg := prometheus.NewPedanticRegistry()
	at, err := activitytracker.NewActivityTracker(activitytracker.Config{Filepath: activityFile, MaxEntries: 1024}, reg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, at.Close()) })

	var gotBody string
	var capturedActivity string
	handlerStarted := make(chan struct{})

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		activities, err := activitytracker.LoadUnfinishedEntries(activityFile)
		require.NoError(t, err)
		require.Len(t, activities, 1)
		capturedActivity = activities[0].Activity
		close(handlerStarted)
		got, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = string(got)
		w.WriteHeader(http.StatusOK)
	})
	handler := NewActivityTrackingMiddleware(at, log.NewNopLogger(), inner)

	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })

	req := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/01ABC/files?path=chunks/000001", pr)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Scope-OrgID", "tenant1")
	resp := httptest.NewRecorder()

	serveHTTPDone := make(chan struct{})
	go func() {
		defer close(serveHTTPDone)
		handler.ServeHTTP(resp, req)
	}()

	select {
	case <-handlerStarted:
		// Handler started before any body data was sent — middleware did not buffer.
	case <-time.After(time.Second):
		t.Fatal("handler did not start before body was sent: middleware is buffering the request body")
	}

	// Send the body only after confirming the handler has already started.
	_, err = pw.Write([]byte(bodyContent))
	require.NoError(t, err)
	require.NoError(t, pw.Close())

	select {
	case <-serveHTTPDone:
	case <-time.After(time.Second):
		t.Fatal("handler did not complete after body was sent")
	}

	require.Equal(t, http.StatusOK, resp.Code)
	require.Equal(t, bodyContent, gotBody)
	// URL query params are captured even though the body was not buffered.
	require.Equal(t, toActivityTrackerString(req, "", map[string][]string{"path": {"chunks/000001"}}), capturedActivity)
}
