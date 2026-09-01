// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier"
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
				return makeTestHTTPRequestFromRemoteRead(makeTestRemoteReadRequest(prompb.ReadRequest_SAMPLES))
			},
			expectedParams: url.Values{
				"start_0":    []string{"0"},
				"end_0":      []string{"42000"},
				"matchers_0": []string{`{__name__="some_metric",foo=~".*bar.*"}`},
				"start_1":    []string{"10000"},
				"end_1":      []string{"20000"},
				"matchers_1": []string{`{__name__="up"}`},
				"hints_1":    []string{`{"step_ms":1000,"start_ms":10000,"end_ms":20000}`},
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

func TestRemoteReadRoundTripperCallsDownstreamOnAll(t *testing.T) {
	testCases := map[string]struct {
		handler                func(next MetricsQueryHandler) MetricsQueryHandler
		expectDownstreamCalled int
		expectMiddlewareCalled int
		expectError            string
	}{
		"skipping middleware": {
			handler: func(next MetricsQueryHandler) MetricsQueryHandler {
				return HandlerFunc(func(ctx context.Context, req MetricsQueryRequest) (Response, error) {
					return next.Do(ctx, req)
				})
			},
			expectDownstreamCalled: 2,
			expectMiddlewareCalled: 2,
		},
		"error middleware": {
			handler: func(next MetricsQueryHandler) MetricsQueryHandler {
				return HandlerFunc(func(ctx context.Context, req MetricsQueryRequest) (Response, error) {
					if req.(*remoteReadQueryRequest).promQuery == "{__name__=\"some_metric\",foo=~\".*bar.*\"}" {
						return nil, apierror.New(apierror.TypeBadData, "TestErrorMiddleware")
					}
					return next.Do(ctx, req)
				})
			},
			expectDownstreamCalled: 1,
			expectMiddlewareCalled: 2,
			expectError:            "remote read error (matchers_0: {__name__=\"some_metric\",foo=~\".*bar.*\"}): TestErrorMiddleware",
		},
	}

	downstream := makeDownstreamRoundTripper(t)

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var actualDownstreamCalls int
			roundTripper := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
				actualDownstreamCalls++
				return downstream.RoundTrip(r)
			})

			actualMiddleWareCalls := 0
			middleware := MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
				actualMiddleWareCalls++
				return tc.handler(next)
			})
			rr := NewRemoteReadRoundTripper(roundTripper, middleware)
			_, err := rr.RoundTrip(makeTestHTTPRequestFromRemoteRead(makeTestRemoteReadRequest(prompb.ReadRequest_SAMPLES)))
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
		expectedStartMs = 11000
		expectedEndMs   = 19000
	)

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

	downstream := makeDownstreamRoundTripper(t)
	origRemoteReadReq := makeTestRemoteReadRequest(prompb.ReadRequest_SAMPLES)

	rr := NewRemoteReadRoundTripper(downstream, middleware)
	resp, err := rr.RoundTrip(makeTestHTTPRequestFromRemoteRead(origRemoteReadReq))

	require.NoError(t, err)

	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "snappy", resp.Header.Get("Content-Encoding"))
	require.Equal(t, "application/x-protobuf", resp.Header.Get("Content-Type"))

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	data, err = snappy.Decode(nil, data)
	require.NoError(t, err)

	var rrResp prompb.ReadResponse
	err = proto.Unmarshal(data, &rrResp)
	require.NoError(t, err)

	require.Equal(t, prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: []*prompb.TimeSeries{
					{
						Labels:  []prompb.Label{{Name: "__name__", Value: "some_metric"}, {Name: "foo", Value: "drop_bar"}},
						Samples: []prompb.Sample{{Timestamp: 15000, Value: 1}},
					},
				},
			},
			{
				Timeseries: []*prompb.TimeSeries{
					{
						Labels:  []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "job", Value: "s1"}},
						Samples: []prompb.Sample{{Timestamp: 15000, Value: 2}},
					},
					{
						Labels:  []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "job", Value: "s2"}},
						Samples: []prompb.Sample{{Timestamp: 15000, Value: 2}},
					},
				},
			},
		},
	}, rrResp)
}

func TestRemoteReadRoundTripper_StreamedXorChunk(t *testing.T) {
	downstream := makeDownstreamRoundTripper(t)
	origRemoteReadReq := makeTestRemoteReadRequest(prompb.ReadRequest_STREAMED_XOR_CHUNKS)

	rr := NewRemoteReadRoundTripper(downstream)
	resp, err := rr.RoundTrip(makeTestHTTPRequestFromRemoteRead(origRemoteReadReq))

	require.NoError(t, err)

	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse", resp.Header.Get("Content-Type"))

	type testSample struct {
		ts  int64
		val float64
	}

	expectedSamplesByLabel := map[string][]testSample{
		"{__name__=\"up\", job=\"s1\"}": {
			{15000, 2},
		},
		"{__name__=\"some_metric\", foo=\"drop_bar\"}": {
			{0, 0},
			{15000, 1},
			{30000, 2},
		},
		"{__name__=\"up\", job=\"s2\"}": {
			{15000, 2},
		},
	}

	computedSamplesByLabel := map[string][]testSample{}

	chunkedReader := remote.NewChunkedReader(resp.Body, math.MaxUint64, nil)
	ss := remote.NewChunkedSeriesSet(chunkedReader, resp.Body, 0, 60*60*1000, func(err error) {
		require.ErrorIs(t, err, io.EOF)
	})

	for ss.Next() {
		require.NoError(t, ss.Err())
		series := ss.At()

		var samples []testSample
		it := series.Iterator(nil)
		for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
			require.Equal(t, typ, chunkenc.ValFloat)
			ts, val := it.At()
			samples = append(samples, testSample{ts, val})
		}

		labels := series.Labels().String()
		computedSamplesByLabel[labels] = samples
	}

	require.Equal(t, expectedSamplesByLabel, computedSamplesByLabel)
}

func TestRemoteReadRoundTripper_ShouldAllowMiddlewaresToReturnEmptyResponse(t *testing.T) {
	// Create a middleware that return an empty response.
	middleware := MetricsQueryMiddlewareFunc(func(_ MetricsQueryHandler) MetricsQueryHandler {
		return HandlerFunc(func(_ context.Context, _ MetricsQueryRequest) (Response, error) {
			return NewEmptyPrometheusResponse(), nil
		})
	})

	// Mock the downstream to capture the received request.
	downstream := makeDownstreamRoundTripper(t)

	rr := NewRemoteReadRoundTripper(downstream, middleware)
	rrReq := makeTestRemoteReadRequest(prompb.ReadRequest_SAMPLES)

	resp, err := rr.RoundTrip(makeTestHTTPRequestFromRemoteRead(rrReq))

	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "snappy", resp.Header.Get("Content-Encoding"))
	require.Equal(t, "application/x-protobuf", resp.Header.Get("Content-Type"))

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	data, err = snappy.Decode(nil, data)
	require.NoError(t, err)

	var rrResp prompb.ReadResponse
	err = proto.Unmarshal(data, &rrResp)
	require.NoError(t, err)

	require.Len(t, rrResp.Results, len(rrReq.Queries))
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
			require.NotSame(t, unsafe.SliceData(actualReq.query.Matchers), unsafe.SliceData(testData.input.query.Matchers))

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

func makeTestRemoteReadRequest(respType prompb.ReadRequest_ResponseType) *prompb.ReadRequest {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"},
					{Name: "foo", Type: prompb.LabelMatcher_RE, Value: ".*bar.*"},
				},
				StartTimestampMs: 0,
				EndTimestampMs:   42000,
				Hints:            nil, // Don't add hints to this query so that we exercise code when the request query has no hints.
			},
			{
				Matchers: []*prompb.LabelMatcher{
					{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "up"},
				},
				StartTimestampMs: 10000,
				EndTimestampMs:   20000,
				Hints: &prompb.ReadHints{
					StartMs: 10000,
					EndMs:   20000,
					StepMs:  1000,
				},
			},
		},
	}
	req.AcceptedResponseTypes = []prompb.ReadRequest_ResponseType{respType}
	return req
}

func makeDownstreamRoundTripper(t *testing.T) http.RoundTripper {
	storage := promqltest.LoadedStorage(t, `
	    load 15s
	        some_metric{foo="drop_bar"} 0+1x10
	        some_metric{foo="drop_rab"} 0+1x10
	        up{job="s1"}                0+2x10
	        up{job="s2"}                0+2x10
	`)

	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	handler := querier.RemoteReadHandler(storage, log.NewNopLogger(), querier.Config{})
	return RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, r)
		return rec.Result(), nil
	})
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
