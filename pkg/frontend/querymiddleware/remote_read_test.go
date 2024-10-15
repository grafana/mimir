// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

var _ = MetricsQueryRequest(&remoteReadQueryRequest{})

func TestParseRemoteReadRequestWithoutConsumingBody(t *testing.T) {
	testCases := map[string]struct {
		makeRequest           func() *http.Request
		contentLength         int
		expectedErrorContains string
		expectedErrorIs       error
		expectedParams        url.Values
	}{
		"no body": {
			makeRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "/api/v1/read", nil)
				req.Body = nil
				return req
			},
			expectedParams: nil,
		},
		"valid body": {
			makeRequest: func() *http.Request {
				return makeTestHTTPRequestFromRemoteRead(makeTestRemoteReadRequest())
			},
			expectedParams: url.Values{
				"start_0":    []string{"0"},
				"end_0":      []string{"42"},
				"matchers_0": []string{`{__name__="some_metric",foo=~".*bar.*"}`},
				"start_1":    []string{"10"},
				"end_1":      []string{"20"},
				"matchers_1": []string{`{__name__="up"}`},
				"hints_1":    []string{`{"step_ms":1000,"start_ms":10,"end_ms":20}`},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			req := tc.makeRequest()
			params, err := ParseRemoteReadRequestValuesWithoutConsumingBody(req)
			if err != nil {
				if tc.expectedErrorIs != nil {
					require.ErrorIs(t, err, tc.expectedErrorIs)
					require.Contains(t, err.Error(), tc.expectedErrorContains)
				} else {
					require.NoError(t, err)
				}
			}
			require.Equal(t, tc.expectedParams, params)

			// Check that we can still read the Body after parsing.
			if req.Body != nil {
				bodyBytes, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.NoError(t, req.Body.Close())
				require.NotEmpty(t, bodyBytes)
			}
		})
	}
}

type mockRoundTripper struct {
	onRoundTrip func(*http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.onRoundTrip(req)
}

type skipMiddleware struct {
}

func (s *skipMiddleware) Do(_ context.Context, _ MetricsQueryRequest) (Response, error) {
	return nil, nil
}

type errorMiddleware struct {
}

func (s *errorMiddleware) Do(_ context.Context, _ MetricsQueryRequest) (Response, error) {
	return nil, apierror.New(apierror.TypeBadData, "TestErrorMiddleware")
}

func TestRemoteReadRoundTripperCallsDownstreamOnAll(t *testing.T) {
	testCases := map[string]struct {
		handler                MetricsQueryHandler
		expectDownstreamCalled int
		expectMiddlewareCalled int
		expectError            string
	}{
		"skipping middleware": {
			handler:                &skipMiddleware{},
			expectDownstreamCalled: 1,
			expectMiddlewareCalled: 2,
		},
		"error middleware": {
			handler:                &errorMiddleware{},
			expectDownstreamCalled: 0,
			expectMiddlewareCalled: 1,
			expectError:            "remote read error (matchers_0: {__name__=\"some_metric\",foo=~\".*bar.*\"}): TestErrorMiddleware",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var actualDownstreamCalls int
			roundTripper := &mockRoundTripper{
				onRoundTrip: func(_ *http.Request) (*http.Response, error) {
					actualDownstreamCalls++
					return nil, nil
				},
			}

			actualMiddleWareCalls := 0
			middleware := MetricsQueryMiddlewareFunc(func(_ MetricsQueryHandler) MetricsQueryHandler {
				actualMiddleWareCalls++
				return tc.handler
			})
			rr := NewRemoteReadRoundTripper(roundTripper, middleware)
			_, err := rr.RoundTrip(makeTestHTTPRequestFromRemoteRead(makeTestRemoteReadRequest()))
			if tc.expectError != "" {
				require.Error(t, err)
				require.Equal(t, tc.expectError, err.Error())
				// The error has to be an apiError to have the correct formatting
				// in the HTTP transport handler. Otherwise the wrapper error
				// is lost. So we check the conversion to HTTP error here.
				response, ok := apierror.HTTPResponseFromError(err)
				require.True(t, ok)
				require.Equal(t, http.StatusBadRequest, int(response.Code))
				apiErr := apiResponse{}
				require.NoError(t, json.Unmarshal(response.Body, &apiErr))
				require.Equal(t, "error", apiErr.Status)
				require.Equal(t, apierror.TypeBadData, apiErr.ErrorType)
				require.Equal(t, tc.expectError, apiErr.Error)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectDownstreamCalled, actualDownstreamCalls)
			require.Equal(t, tc.expectMiddlewareCalled, actualMiddleWareCalls)
		})
	}
}

type apiResponse struct {
	Status    string        `json:"status"`
	ErrorType apierror.Type `json:"errorType,omitempty"`
	Error     string        `json:"error,omitempty"`
}

func TestRemoteReadRoundTripper_ShouldAllowMiddlewaresToManipulateRequest(t *testing.T) {
	const (
		expectedStartMs = 11
		expectedEndMs   = 19
	)

	origRemoteReadReq := makeTestRemoteReadRequest()

	// Create a middleware that manipulate the query start/end timestamps.
	middleware := MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return HandlerFunc(func(ctx context.Context, req MetricsQueryRequest) (Response, error) {
			req, err := req.WithStartEnd(expectedStartMs, expectedEndMs)
			if err != nil {
				return nil, err
			}

			return next.Do(ctx, req)
		})
	})

	// Mock the downstream to capture the received request.
	var downstreamReq *http.Request
	downstream := &mockRoundTripper{
		onRoundTrip: func(req *http.Request) (*http.Response, error) {
			downstreamReq = req
			return nil, nil
		},
	}

	rr := NewRemoteReadRoundTripper(downstream, middleware)
	_, err := rr.RoundTrip(makeTestHTTPRequestFromRemoteRead(origRemoteReadReq))
	require.NoError(t, err)
	require.NotNil(t, downstreamReq)

	// Ensure the downstream HTTP request has been correctly manipulated.
	require.Equal(t, strconv.Itoa(int(downstreamReq.ContentLength)), downstreamReq.Header.Get("Content-Length")) // The two should match.
	require.Equal(t, "snappy", downstreamReq.Header.Get("Content-Encoding"))

	// Parse the HTTP request received by the downstream.
	downstreamRemoteReadReq, err := unmarshalRemoteReadRequest(downstreamReq.Context(), downstreamReq.Body, int(downstreamReq.ContentLength))
	require.NoError(t, err)
	require.Len(t, downstreamRemoteReadReq.Queries, len(origRemoteReadReq.Queries))

	// Ensure the downstream received the manipulated start/end timestamps.
	for i, query := range downstreamRemoteReadReq.Queries {
		require.Equal(t, int64(expectedStartMs), query.StartTimestampMs)
		require.Equal(t, int64(expectedEndMs), query.EndTimestampMs)

		if origRemoteReadReq.Queries[i].Hints != nil {
			require.NotNil(t, query.Hints)
			require.Equal(t, int64(expectedStartMs), query.Hints.StartMs)
			require.Equal(t, int64(expectedEndMs), query.Hints.EndMs)
		}
	}

	// Excluding the start/end timestamps, everything else should be equal.
	// To run this comparison we override the start/end timestamp both in the original and downstream request.
	for _, req := range []*prompb.ReadRequest{origRemoteReadReq, downstreamRemoteReadReq} {
		for _, query := range req.Queries {
			query.StartTimestampMs = 0
			query.EndTimestampMs = 0

			if query.Hints != nil {
				query.Hints.StartMs = 0
				query.Hints.EndMs = 0
			}
		}
	}

	require.Equal(t, origRemoteReadReq, downstreamRemoteReadReq)
}

func TestRemoteReadRoundTripper_ShouldAllowMiddlewaresToReturnEmptyResponse(t *testing.T) {
	// Create a middleware that return an empty response.
	middleware := MetricsQueryMiddlewareFunc(func(_ MetricsQueryHandler) MetricsQueryHandler {
		return HandlerFunc(func(_ context.Context, _ MetricsQueryRequest) (Response, error) {
			return newEmptyPrometheusResponse(), nil
		})
	})

	// Mock the downstream to capture the received request.
	var downstreamReq *http.Request
	downstream := &mockRoundTripper{
		onRoundTrip: func(req *http.Request) (*http.Response, error) {
			downstreamReq = req
			return nil, nil
		},
	}

	rr := NewRemoteReadRoundTripper(downstream, middleware)
	origRemoteReadReq := makeTestRemoteReadRequest()

	_, err := rr.RoundTrip(makeTestHTTPRequestFromRemoteRead(origRemoteReadReq))
	require.NoError(t, err)
	require.NotNil(t, downstreamReq)

	// Ensure the HTTP request received by the downstream is equal to the original one.
	downstreamRemoteReadReq, err := unmarshalRemoteReadRequest(downstreamReq.Context(), downstreamReq.Body, int(downstreamReq.ContentLength))
	require.NoError(t, err)
	require.Equal(t, origRemoteReadReq, downstreamRemoteReadReq)
}

func TestRemoteReadQueryRequest_WithStartEnd(t *testing.T) {
	const (
		updatedStartMs = 1100
		updatedEndMs   = 1200
	)

	tests := map[string]struct {
		input    *remoteReadQueryRequest
		expected *remoteReadQueryRequest
	}{
		"without hints": {
			input: &remoteReadQueryRequest{
				path:      remoteReadPathSuffix,
				promQuery: `{pod="pod-1"}`,
				query: &prompb.Query{
					StartTimestampMs: updatedStartMs - 100,
					EndTimestampMs:   updatedEndMs + 100,
					Matchers:         []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "pod", Value: "pod-1"}},
				},
			},
			expected: &remoteReadQueryRequest{
				path:      remoteReadPathSuffix,
				promQuery: `{pod="pod-1"}`,
				query: &prompb.Query{
					StartTimestampMs: updatedStartMs,
					EndTimestampMs:   updatedEndMs,
					Matchers:         []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "pod", Value: "pod-1"}},
				},
			},
		},
		"with hints with start/end range larger than the new requested start/end range": {
			input: &remoteReadQueryRequest{
				path:      remoteReadPathSuffix,
				promQuery: `{pod="pod-1"}`,
				query: &prompb.Query{
					StartTimestampMs: updatedStartMs - 100,
					EndTimestampMs:   updatedEndMs + 100,
					Matchers:         []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "pod", Value: "pod-1"}},
					Hints: &prompb.ReadHints{
						StepMs:   123,
						Func:     "series",
						StartMs:  updatedStartMs - 100,
						EndMs:    updatedEndMs + 100,
						Grouping: []string{"cluster"},
						By:       true,
					},
				},
			},
			expected: &remoteReadQueryRequest{
				path:      remoteReadPathSuffix,
				promQuery: `{pod="pod-1"}`,
				query: &prompb.Query{
					StartTimestampMs: updatedStartMs,
					EndTimestampMs:   updatedEndMs,
					Matchers:         []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "pod", Value: "pod-1"}},
					Hints: &prompb.ReadHints{
						StepMs:   123,
						Func:     "series",
						StartMs:  updatedStartMs,
						EndMs:    updatedEndMs,
						Grouping: []string{"cluster"},
						By:       true,
					},
				},
			},
		},
		"with hints with start/end range smaller than the new requested start/end range": {
			input: &remoteReadQueryRequest{
				path:      remoteReadPathSuffix,
				promQuery: `{pod="pod-1"}`,
				query: &prompb.Query{
					StartTimestampMs: updatedStartMs - 100,
					EndTimestampMs:   updatedEndMs + 100,
					Matchers:         []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "pod", Value: "pod-1"}},
					Hints: &prompb.ReadHints{
						StepMs:   123,
						Func:     "series",
						StartMs:  updatedStartMs + 10,
						EndMs:    updatedEndMs - 10,
						Grouping: []string{"cluster"},
						By:       true,
					},
				},
			},
			expected: &remoteReadQueryRequest{
				path:      remoteReadPathSuffix,
				promQuery: `{pod="pod-1"}`,
				query: &prompb.Query{
					StartTimestampMs: updatedStartMs,
					EndTimestampMs:   updatedEndMs,
					Matchers:         []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "pod", Value: "pod-1"}},
					Hints: &prompb.ReadHints{
						StepMs:   123,
						Func:     "series",
						StartMs:  updatedStartMs + 10,
						EndMs:    updatedEndMs - 10,
						Grouping: []string{"cluster"},
						By:       true,
					},
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual, err := testData.input.WithStartEnd(updatedStartMs, updatedEndMs)
			require.NoError(t, err)
			require.NotSame(t, actual, testData.input)
			require.Equal(t, testData.expected, actual)

			// Ensure it's a deep copy.
			actualReq, ok := actual.(*remoteReadQueryRequest)
			require.True(t, ok)
			require.NotSame(t, actualReq.query, testData.input.query)
			require.NotSame(t, actualReq.query.Matchers, testData.input.query.Matchers)

			if actualReq.query.Hints != nil {
				require.NotSame(t, actualReq.query.Hints, testData.input.query.Hints)
			}

			for i, actualMatcher := range actualReq.query.Matchers {
				require.NotSame(t, actualMatcher, testData.input.query.Matchers[i])
			}
		})
	}
}

func makeTestHTTPRequestFromRemoteRead(readReq *prompb.ReadRequest) *http.Request {
	request := httptest.NewRequest("GET", "/api/v1/read", nil)
	request.Header.Add("User-Agent", "test-user-agent")
	request.Header.Add("Content-Type", "application/x-protobuf")
	request.Header.Add("Content-Encoding", "snappy")
	data, _ := proto.Marshal(readReq) // Ignore error, if this fails, the test will fail.
	compressed := snappy.Encode(nil, data)
	request.Body = io.NopCloser(bytes.NewReader(compressed))

	return request
}

func makeTestRemoteReadRequest() *prompb.ReadRequest {
	return &prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"},
					{Name: "foo", Type: prompb.LabelMatcher_RE, Value: ".*bar.*"},
				},
				StartTimestampMs: 0,
				EndTimestampMs:   42,
				Hints:            nil, // Don't add hints to this query so that we exercise code when the request query has no hints.
			},
			{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "up"},
				},
				StartTimestampMs: 10,
				EndTimestampMs:   20,
				Hints: &prompb.ReadHints{
					StartMs: 10,
					EndMs:   20,
					StepMs:  1000,
				},
			},
		},
	}
}

// This is not a full test yet, only tests what's needed for the query blocker and stats.
func TestRemoteReadToMetricsQueryRequest(t *testing.T) {
	testCases := map[string]struct {
		query         *prompb.Query
		expectedQuery string
		expectedStep  int64
		expectedStart int64
		expectedEnd   int64
		expectedMinT  int64
		expectedMaxT  int64
	}{
		"query without hints": {
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"},
					{Name: "foo", Type: prompb.LabelMatcher_RE, Value: ".*bar.*"},
				},
				StartTimestampMs: 10,
				EndTimestampMs:   20,
			},
			expectedQuery: "{__name__=\"some_metric\",foo=~\".*bar.*\"}",
			expectedStep:  0,
			expectedStart: 10,
			expectedEnd:   20,
			expectedMinT:  10,
			expectedMaxT:  20,
		},
		"query with hints": {
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "up"},
				},
				StartTimestampMs: 10,
				EndTimestampMs:   20,
				Hints: &prompb.ReadHints{
					StartMs: 5,
					EndMs:   25,
					StepMs:  1000,
				},
			},
			expectedQuery: "{__name__=\"up\"}",
			expectedStep:  0,
			expectedStart: 10,
			expectedEnd:   20,
			expectedMinT:  5,
			expectedMaxT:  25,
		},
		"query with zero-value hints": {
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "up"},
				},
				StartTimestampMs: 10,
				EndTimestampMs:   20,
				Hints:            &prompb.ReadHints{},
			},
			expectedQuery: "{__name__=\"up\"}",
			expectedStep:  0,
			expectedStart: 10,
			expectedEnd:   20,
			expectedMinT:  10,
			expectedMaxT:  20,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			metricsQR, err := remoteReadToMetricsQueryRequest("something", tc.query)
			require.NoError(t, err)
			require.Equal(t, tc.expectedQuery, metricsQR.GetQuery())
			require.Equal(t, tc.expectedStep, metricsQR.GetStep())
			require.Equal(t, tc.expectedStart, metricsQR.GetStart())
			require.Equal(t, tc.expectedEnd, metricsQR.GetEnd())
			require.Equal(t, tc.expectedMinT, metricsQR.GetMinT())
			require.Equal(t, tc.expectedMaxT, metricsQR.GetMaxT())
		})
	}
}
