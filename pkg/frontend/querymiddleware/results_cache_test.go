// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/results_cache_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestResultsCacheConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg      ResultsCacheConfig
		expected error
	}{
		"should pass with default config": {
			cfg: func() ResultsCacheConfig {
				cfg := ResultsCacheConfig{}
				flagext.DefaultValues(&cfg)

				return cfg
			}(),
		},
		"should pass with memcached backend": {
			cfg: func() ResultsCacheConfig {
				cfg := ResultsCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = cache.BackendMemcached
				cfg.Memcached.Addresses = []string{"localhost"}

				return cfg
			}(),
		},
		"should fail with invalid memcached config": {
			cfg: func() ResultsCacheConfig {
				cfg := ResultsCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = cache.BackendMemcached
				cfg.Memcached.Addresses = []string{}

				return cfg
			}(),
			expected: cache.ErrNoMemcachedAddresses,
		},
		"should fail with unsupported backend": {
			cfg: func() ResultsCacheConfig {
				cfg := ResultsCacheConfig{}
				flagext.DefaultValues(&cfg)

				cfg.Backend = "unsupported"

				return cfg
			}(),
			expected: errUnsupportedBackend,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			if testData.expected != nil {
				assert.ErrorIs(t, testData.cfg.Validate(), testData.expected)
			} else {
				assert.NoError(t, testData.cfg.Validate())
			}
		})
	}
}

func mkAPIResponse(start, end, step int64) *PrometheusResponse {
	var samples []mimirpb.Sample
	for i := start; i <= end; i += step {
		samples = append(samples, mimirpb.Sample{
			TimestampMs: i,
			Value:       float64(i),
		})
	}

	return &PrometheusResponse{
		Status: statusSuccess,
		Data: &PrometheusData{
			ResultType: matrix,
			Result: []SampleStream{
				{
					Labels: []mimirpb.LabelAdapter{
						{Name: "foo", Value: "bar"},
					},
					Samples: samples,
				},
			},
		},
	}
}

func mkExtent(start, end int64) Extent {
	return mkExtentWithStepAndQueryTime(start, end, 10, 0)
}

func mkExtentWithStepAndQueryTime(start, end, step, queryTime int64) Extent {
	res := mkAPIResponse(start, end, step)
	marshalled, err := types.MarshalAny(res)
	if err != nil {
		panic(err)
	}
	return Extent{
		Start:            start,
		End:              end,
		Response:         marshalled,
		QueryTimestampMs: queryTime,
	}
}

func TestIsRequestCachable(t *testing.T) {
	maxCacheTime := int64(150 * 1000)

	for _, tc := range []struct {
		name                      string
		request                   MetricsQueryRequest
		expected                  bool
		expectedNotCachableReason string
		cacheStepUnaligned        bool
	}{
		// @ modifier on vector selectors.
		{
			name:     "@ modifier on vector selector, before end, before maxCacheTime",
			request:  &PrometheusRangeQueryRequest{Query: "metric @ 123", End: 125000, Step: 5},
			expected: true,
		},
		{
			name:                      "@ modifier on vector selector, after end, before maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "metric @ 127", End: 125000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		{
			name:                      "@ modifier on vector selector, before end, after maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "metric @ 151", End: 200000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		{
			name:                      "@ modifier on vector selector, after end, after maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "metric @ 151", End: 125000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		{
			name:     "@ modifier on vector selector with start() before maxCacheTime",
			request:  &PrometheusRangeQueryRequest{Query: "metric @ start()", Start: 100000, End: 200000, Step: 5},
			expected: true,
		},
		{
			name:                      "@ modifier on vector selector with end() after maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "metric @ end()", Start: 100000, End: 200000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		// @ modifier on matrix selectors.
		{
			name:     "@ modifier on matrix selector, before end, before maxCacheTime",
			request:  &PrometheusRangeQueryRequest{Query: "rate(metric[5m] @ 123)", End: 125000, Step: 5},
			expected: true,
		},
		{
			name:                      "@ modifier on matrix selector, after end, before maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "rate(metric[5m] @ 127)", End: 125000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		{
			name:                      "@ modifier on matrix selector, before end, after maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "rate(metric[5m] @ 151)", End: 200000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		{
			name:                      "@ modifier on matrix selector, after end, after maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "rate(metric[5m] @ 151)", End: 125000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		{
			name:     "@ modifier on matrix selector with start() before maxCacheTime",
			request:  &PrometheusRangeQueryRequest{Query: "rate(metric[5m] @ start())", Start: 100000, End: 200000, Step: 5},
			expected: true,
		},
		{
			name:                      "@ modifier on matrix selector with end() after maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "rate(metric[5m] @ end())", Start: 100000, End: 200000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		// @ modifier on subqueries.
		{
			name:     "@ modifier on subqueries, before end, before maxCacheTime",
			request:  &PrometheusRangeQueryRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ 123)", End: 125000, Step: 5},
			expected: true,
		},
		{
			name:                      "@ modifier on subqueries, after end, before maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ 127)", End: 125000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		{
			name:                      "@ modifier on subqueries, before end, after maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ 151)", End: 200000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		{
			name:                      "@ modifier on subqueries, after end, after maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ 151)", End: 125000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		{
			name:     "@ modifier on subqueries with start() before maxCacheTime",
			request:  &PrometheusRangeQueryRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ start())", Start: 100000, End: 200000, Step: 5},
			expected: true,
		},
		{
			name:                      "@ modifier on subqueries with end() after maxCacheTime",
			request:                   &PrometheusRangeQueryRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] @ end())", Start: 100000, End: 200000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		// offset modifier on vector selectors.
		{
			name:     "positive offset on vector selector",
			request:  &PrometheusRangeQueryRequest{Query: "metric offset 1ms", End: 200000, Step: 5},
			expected: true,
		},
		{
			name:                      "negative offset on vector selector",
			request:                   &PrometheusRangeQueryRequest{Query: "metric offset -1ms", End: 125000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		// offset modifier on subqueries.
		{
			name:     "positive offset on subqueries",
			request:  &PrometheusRangeQueryRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] offset 1ms)", Start: 100000, End: 200000, Step: 5},
			expected: true,
		},
		{
			name:                      "negative offset on subqueries",
			request:                   &PrometheusRangeQueryRequest{Query: "sum_over_time(rate(metric[1m])[10m:1m] offset -1ms)", Start: 100000, End: 200000, Step: 5},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonModifiersNotCachable,
		},
		// On step aligned and non-aligned requests
		{
			name:     "request that is step aligned",
			request:  &PrometheusRangeQueryRequest{Query: "query", Start: 100000, End: 200000, Step: 10},
			expected: true,
		},
		{
			name:                      "request that is NOT step aligned, with cacheStepUnaligned=false",
			request:                   &PrometheusRangeQueryRequest{Query: "query", Start: 100000, End: 200000, Step: 3},
			expected:                  false,
			expectedNotCachableReason: notCachableReasonUnalignedTimeRange,
			cacheStepUnaligned:        false,
		},
		{
			name:               "request that is NOT step aligned",
			request:            &PrometheusRangeQueryRequest{Query: "query", Start: 100000, End: 200000, Step: 3},
			expected:           true,
			cacheStepUnaligned: true,
		},
	} {
		{
			t.Run(tc.name, func(t *testing.T) {
				cachable, reason := isRequestCachable(tc.request, maxCacheTime, tc.cacheStepUnaligned, log.NewNopLogger())
				require.Equal(t, tc.expected, cachable)
				if !cachable {
					require.Equal(t, tc.expectedNotCachableReason, reason)
				} else {
					require.Empty(t, reason)
				}
			})
		}
	}
}

func TestIsResponseCachable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response Response
		expected bool
	}{
		// Tests only for cacheControlHeader
		{
			name: "does not contain the cacheControl header",
			response: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   "meaninglessheader",
						Values: []string{},
					},
				},
			}),
			expected: true,
		},
		{
			name: "does contain the cacheControl header which has the value",
			response: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   cacheControlHeader,
						Values: []string{noStoreValue},
					},
				},
			}),
			expected: false,
		},
		{
			name: "cacheControl header contains extra values but still good",
			response: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{
					{
						Name:   cacheControlHeader,
						Values: []string{"foo", noStoreValue},
					},
				},
			}),
			expected: false,
		},
		{
			name:     "broken response",
			response: Response(&PrometheusResponse{}),
			expected: true,
		},
		{
			name: "nil headers",
			response: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{nil},
			}),
			expected: true,
		},
		{
			name: "had cacheControl header but no values",
			response: Response(&PrometheusResponse{
				Headers: []*PrometheusResponseHeader{{Name: cacheControlHeader}},
			}),
			expected: true,
		},
	} {
		{
			t.Run(tc.name, func(t *testing.T) {
				ret := isResponseCachable(tc.response, log.NewNopLogger())
				require.Equal(t, tc.expected, ret)
			})
		}
	}
}

func TestPartitionCacheExtents(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		input                  MetricsQueryRequest
		prevCachedResponse     []Extent
		expectedRequests       []MetricsQueryRequest
		expectedCachedResponse []Response
	}{
		{
			name: "Test a complete hit.",
			input: &PrometheusRangeQueryRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
			prevCachedResponse: []Extent{
				mkExtent(0, 100),
			},
			expectedCachedResponse: []Response{
				mkAPIResponse(0, 100, 10),
			},
		},

		{
			name: "Test with a complete miss.",
			input: &PrometheusRangeQueryRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
			prevCachedResponse: []Extent{
				mkExtent(110, 210),
			},
			expectedRequests: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{
					Start: 0,
					End:   100,
					Step:  10,
				},
			},
		},
		{
			name: "Test a partial hit.",
			input: &PrometheusRangeQueryRequest{
				Start: 0,
				End:   100,
				Step:  10,
			},
			prevCachedResponse: []Extent{
				mkExtent(50, 100),
			},
			expectedRequests: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{
					Start: 0,
					End:   50,
					Step:  10,
				},
			},
			expectedCachedResponse: []Response{
				mkAPIResponse(50, 100, 10),
			},
		},
		{
			name: "Test multiple partial hits.",
			input: &PrometheusRangeQueryRequest{
				Start: 100,
				End:   200,
				Step:  10,
			},
			prevCachedResponse: []Extent{
				mkExtent(50, 120),
				mkExtent(160, 250),
			},
			expectedRequests: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{
					Start: 120,
					End:   160,
					Step:  10,
				},
			},
			expectedCachedResponse: []Response{
				mkAPIResponse(100, 120, 10),
				mkAPIResponse(160, 200, 10),
			},
		},
		{
			name: "Partial hits with tiny gap.",
			input: &PrometheusRangeQueryRequest{
				Start: 100,
				End:   160,
				Step:  10,
			},
			prevCachedResponse: []Extent{
				mkExtent(50, 120),
				mkExtent(122, 130),
			},
			expectedRequests: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{
					Start: 120,
					End:   160,
					Step:  10,
				},
			},
			expectedCachedResponse: []Response{
				mkAPIResponse(100, 120, 10),
			},
		},
		{
			name: "Extent is outside the range and the request has a single step (same start and end).",
			input: &PrometheusRangeQueryRequest{
				Start: 100,
				End:   100,
				Step:  10,
			},
			prevCachedResponse: []Extent{
				mkExtent(50, 90),
			},
			expectedRequests: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{
					Start: 100,
					End:   100,
					Step:  10,
				},
			},
		},
		{
			name: "Test when hit has a large step and only a single sample extent.",
			// If there is a only a single sample in the split interval, start and end will be the same.
			input: &PrometheusRangeQueryRequest{
				Start: 100,
				End:   100,
				Step:  10,
			},
			prevCachedResponse: []Extent{
				mkExtent(100, 100),
			},
			expectedCachedResponse: []Response{
				mkAPIResponse(100, 105, 10),
			},
		},

		{
			name: "Start time of all requests must have the same offset into the step.",
			input: &PrometheusRangeQueryRequest{
				Start: 123, // 123 % 33 = 24
				End:   1000,
				Step:  33,
			},
			prevCachedResponse: []Extent{
				// 486 is equal to input.Start + N * input.Step (for integer N)
				// 625 is not equal to input.Start + N * input.Step for any integer N.
				mkExtentWithStepAndQueryTime(486, 625, 33, time.Now().UnixMilli()),
			},
			expectedCachedResponse: []Response{
				mkAPIResponse(486, 625, 33),
			},
			expectedRequests: []MetricsQueryRequest{
				&PrometheusRangeQueryRequest{Start: 123, End: 486, Step: 33},
				&PrometheusRangeQueryRequest{
					Start: 651,  // next number after 625 (end of extent) such that it is equal to input.Start + N * input.Step.
					End:   1000, // until the end
					Step:  33,   // unchanged
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			extractor := PrometheusResponseExtractor{}
			minCacheExtent := int64(10)

			reqs, resps, err := partitionCacheExtents(tc.input, tc.prevCachedResponse, minCacheExtent, extractor)
			require.Nil(t, err)
			require.Equal(t, tc.expectedRequests, reqs)
			require.Equal(t, tc.expectedCachedResponse, resps)

			for _, req := range reqs {
				assert.Equal(t, tc.input.GetStep(), req.GetStep())
				assert.Equal(t, tc.input.GetStart()%tc.input.GetStep(), req.GetStart()%req.GetStep())
			}
		})
	}
}

func TestDefaultSplitter_QueryRequest(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewPedanticRegistry()
	codec := NewPrometheusCodec(reg, formatJSON)

	ctx := context.Background()

	tests := []struct {
		name     string
		r        MetricsQueryRequest
		interval time.Duration
		want     string
	}{
		{"0", &PrometheusRangeQueryRequest{Start: 0, Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:0"},
		{"<30m", &PrometheusRangeQueryRequest{Start: toMs(10 * time.Minute), Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:0"},
		{"30m", &PrometheusRangeQueryRequest{Start: toMs(30 * time.Minute), Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:1"},
		{"91m", &PrometheusRangeQueryRequest{Start: toMs(91 * time.Minute), Step: 10, Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:10:3"},
		{"91m_5m", &PrometheusRangeQueryRequest{Start: toMs(91 * time.Minute), Step: 5 * time.Minute.Milliseconds(), Query: "foo{}"}, 30 * time.Minute, "fake:foo{}:300000:3:60000"},
		{"0", &PrometheusRangeQueryRequest{Start: 0, Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:0"},
		{"<1d", &PrometheusRangeQueryRequest{Start: toMs(22 * time.Hour), Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:0"},
		{"4d", &PrometheusRangeQueryRequest{Start: toMs(4 * 24 * time.Hour), Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:4"},
		{"3d5h", &PrometheusRangeQueryRequest{Start: toMs(77 * time.Hour), Step: 10, Query: "foo{}"}, 24 * time.Hour, "fake:foo{}:10:3"},
		{"1111m", &PrometheusRangeQueryRequest{Start: 1111 * time.Minute.Milliseconds(), Step: 10 * time.Minute.Milliseconds(), Query: "foo{}"}, 1 * time.Hour, "fake:foo{}:600000:18:60000"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s - %s", tt.name, tt.interval), func(t *testing.T) {
			if got := (DefaultCacheKeyGenerator{codec: codec, interval: tt.interval}).QueryRequest(ctx, "fake", tt.r); got != tt.want {
				t.Errorf("generateKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func toMs(t time.Duration) int64 {
	return int64(t / time.Millisecond)
}
