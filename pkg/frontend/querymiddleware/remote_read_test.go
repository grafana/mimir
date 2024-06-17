// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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
			makeRequest: generateTestRemoteReadRequest,
			expectedParams: url.Values{
				"start_0":    []string{"0"},
				"end_0":      []string{"42"},
				"matchers_0": []string{"{__name__=\"some_metric\",foo=~\".*bar.*\"}"},
				"start_1":    []string{"10"},
				"end_1":      []string{"20"},
				"matchers_1": []string{"{__name__=\"up\"}"},
				"hints_1":    []string{"{\"step_ms\":1000}"},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			req := tc.makeRequest()
			params, err := ParseRemoteReadRequestWithoutConsumingBody(req)
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
	called int
}

func (m *mockRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	m.called++
	return nil, nil
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
			roundTripper := &mockRoundTripper{}
			countMiddleWareCalls := 0
			middleware := MetricsQueryMiddlewareFunc(func(MetricsQueryHandler) MetricsQueryHandler {
				countMiddleWareCalls++
				return tc.handler
			})
			rr := newRemoteReadRoundTripper(roundTripper, middleware)
			_, err := rr.RoundTrip(generateTestRemoteReadRequest())
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
			require.Equal(t, tc.expectDownstreamCalled, roundTripper.called)
			require.Equal(t, tc.expectMiddlewareCalled, countMiddleWareCalls)
		})
	}
}

type apiResponse struct {
	Status    string        `json:"status"`
	ErrorType apierror.Type `json:"errorType,omitempty"`
	Error     string        `json:"error,omitempty"`
}

func generateTestRemoteReadRequest() *http.Request {
	request := httptest.NewRequest("GET", "/api/v1/read", nil)
	request.Header.Add("User-Agent", "test-user-agent")
	request.Header.Add("Content-Type", "application/x-protobuf")
	request.Header.Add("Content-Encoding", "snappy")
	remoteReadRequest := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"},
					{Name: "foo", Type: prompb.LabelMatcher_RE, Value: ".*bar.*"},
				},
				StartTimestampMs: 0,
				EndTimestampMs:   42,
			},
			{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "up"},
				},
				StartTimestampMs: 10,
				EndTimestampMs:   20,
				Hints: &prompb.ReadHints{
					StepMs: 1000,
				},
			},
		},
	}
	data, _ := proto.Marshal(remoteReadRequest) // Ignore error, if this fails, the test will fail.
	compressed := snappy.Encode(nil, data)
	request.Body = io.NopCloser(bytes.NewReader(compressed))

	return request
}

// This is not a full test yet, only tests what's needed for the query blocker.
func TestRemoteReadToMetricsQueryRequest(t *testing.T) {
	remoteReadRequest := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"},
					{Name: "foo", Type: prompb.LabelMatcher_RE, Value: ".*bar.*"},
				},
				StartTimestampMs: 10,
				EndTimestampMs:   20,
			},
			{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "up"},
				},
				Hints: &prompb.ReadHints{
					StepMs: 1000,
				},
			},
		},
	}

	expectedGetQuery := []string{
		"{__name__=\"some_metric\",foo=~\".*bar.*\"}",
		"{__name__=\"up\"}",
	}

	for i, query := range remoteReadRequest.Queries {
		metricsQR, err := remoteReadToMetricsQueryRequest("something", query)
		require.NoError(t, err)
		require.Equal(t, expectedGetQuery[i], metricsQR.GetQuery())
	}
}
