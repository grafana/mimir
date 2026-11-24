// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimirtest "github.com/grafana/mimir/pkg/util/test"
)

func TestLabelsQueryCache_RoundTrip(t *testing.T) {
	testGenericQueryCacheRoundTrip(t, newLabelsQueryCacheRoundTripper, "label_names_and_values", map[string]testGenericQueryCacheRequestType{
		"label names request": {
			reqPath:        "/prometheus/api/v1/labels",
			reqData:        url.Values{"start": []string{"2023-07-05T01:00:00Z"}, "end": []string{"2023-07-05T08:00:00Z"}, "match[]": []string{`{job="test_1"}`, `{job!="test_2"}`}},
			cacheKey:       "user-1:1688515200000\x001688544000000\x00{job!=\"test_2\"},{job=\"test_1\"}",
			hashedCacheKey: labelNamesQueryCachePrefix + hashCacheKey("user-1:1688515200000\x001688544000000\x00{job!=\"test_2\"},{job=\"test_1\"}"),
		},
		"label values request": {
			reqPath:        "/prometheus/api/v1/label/test/values",
			reqData:        url.Values{"start": []string{"2023-07-05T01:00:00Z"}, "end": []string{"2023-07-05T08:00:00Z"}, "match[]": []string{`{job="test_1"}`, `{job!="test_2"}`}},
			cacheKey:       "user-1:1688515200000\x001688544000000\x00test\x00{job!=\"test_2\"},{job=\"test_1\"}",
			hashedCacheKey: labelValuesQueryCachePrefix + hashCacheKey("user-1:1688515200000\x001688544000000\x00test\x00{job!=\"test_2\"},{job=\"test_1\"}"),
		},
	})
}

func TestLabelsQueryCache_MetadataDefaultTimeRange(t *testing.T) {
	const userID = "user-1"

	type request struct {
		method string
		path   string
		params url.Values
	}
	newRequest := func(t *testing.T, r request) *http.Request {
		var (
			req *http.Request
			err error
		)
		switch r.method {
		case http.MethodGet:
			req, err = http.NewRequest(r.method, r.path+"?"+r.params.Encode(), nil)
		case http.MethodPost:
			req, err = http.NewRequest(r.method, r.path, strings.NewReader(r.params.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		default:
			t.Fatalf("unsupported HTTP method %q", r.method)
		}
		require.NoError(t, err)
		return req.WithContext(user.InjectOrgID(t.Context(), userID))
	}

	const (
		labelNamesPath  = "/prometheus/api/v1/labels"
		labelValuesPath = "/prometheus/api/v1/label/job/values"
	)
	var (
		noStartAndNoEnd = url.Values{}
		onlyStart       = url.Values{"start": []string{"2023-07-05T01:00:00Z"}}
		onlyEnd         = url.Values{"end": []string{"2023-07-05T08:00:00Z"}}
		startAndEnd     = url.Values{"start": []string{"2023-07-05T01:00:00Z"}, "end": []string{"2023-07-05T08:00:00Z"}}
	)

	tests := map[string]struct {
		metadataDefaultTimeRange time.Duration
		requests                 []request
		expectedDownstreamCalls  int
		expectedCached           bool
	}{
		"limit disabled, no start and no end": {
			requests: []request{
				{method: http.MethodGet, path: labelNamesPath, params: noStartAndNoEnd},
				{method: http.MethodGet, path: labelNamesPath, params: noStartAndNoEnd},
			},
			expectedDownstreamCalls: 1,
			expectedCached:          true,
		},
		"limit enabled, start and end": {
			metadataDefaultTimeRange: time.Hour,
			requests: []request{
				{method: http.MethodGet, path: labelValuesPath, params: startAndEnd},
				{method: http.MethodPost, path: labelValuesPath, params: startAndEnd},
			},
			expectedDownstreamCalls: 1,
			expectedCached:          true,
		},
		"limit enabled, no start and no end": {
			metadataDefaultTimeRange: time.Hour,
			requests: []request{
				{method: http.MethodGet, path: labelNamesPath, params: noStartAndNoEnd},
				{method: http.MethodGet, path: labelNamesPath, params: noStartAndNoEnd},
			},
			expectedDownstreamCalls: 2,
		},
		"limit enabled, only start": {
			metadataDefaultTimeRange: time.Hour,
			requests: []request{
				{method: http.MethodGet, path: labelValuesPath, params: onlyStart},
				{method: http.MethodGet, path: labelValuesPath, params: onlyStart},
			},
			expectedDownstreamCalls: 2,
		},
		"limit enabled, only end in a POST form": {
			metadataDefaultTimeRange: time.Hour,
			requests: []request{
				{method: http.MethodPost, path: labelNamesPath, params: onlyEnd},
				{method: http.MethodPost, path: labelNamesPath, params: onlyEnd},
			},
			expectedDownstreamCalls: 2,
		},
		// Without the limit, these requests have the same cache key, because the key keeps start at MinTime and
		// rounds both ends up to 04:00. With the limit, they query different ranges and blocks.
		"limit enabled, only end at two times that round to the same block boundary": {
			metadataDefaultTimeRange: time.Hour,
			requests: []request{
				{method: http.MethodGet, path: labelValuesPath, params: url.Values{"end": []string{"2023-07-05T02:30:00Z"}}},
				{method: http.MethodGet, path: labelValuesPath, params: url.Values{"end": []string{"2023-07-05T03:30:00Z"}}},
			},
			expectedDownstreamCalls: 2,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cacheBackend := cache.NewInstrumentedMockCache()
			downstreamCalls := 0
			downstream := RoundTripFunc(func(*http.Request) (*http.Response, error) {
				downstreamCalls++
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"status":"success","data":[]}`)),
					Header:     http.Header{"Content-Type": []string{"application/json"}},
				}, nil
			})
			limits := multiTenantMockLimits{byTenant: map[string]mockLimits{
				userID: {resultsCacheTTLForLabelsQuery: time.Minute, metadataDefaultTimeRange: tc.metadataDefaultTimeRange},
			}}
			generator := DefaultCacheKeyGenerator{codec: newTestCodec()}
			rt := newLabelsQueryCacheRoundTripper(cacheBackend, generator, limits, downstream, mimirtest.NewTestingLogger(t), prometheus.NewPedanticRegistry())

			for _, r := range tc.requests {
				res, err := rt.RoundTrip(newRequest(t, r))
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, res.StatusCode)
				_, err = io.ReadAll(res.Body)
				require.NoError(t, err)
			}

			assert.Equal(t, tc.expectedDownstreamCalls, downstreamCalls)
			if !tc.expectedCached {
				assert.Empty(t, cacheBackend.GetItems())
				return
			}
			// The limit does not change the cache key of a request that the cache stores.
			key, err := generator.LabelValues(newRequest(t, tc.requests[0]))
			require.NoError(t, err)
			_, hashedCacheKey := generateGenericQueryRequestCacheKey([]string{userID}, key)
			items := cacheBackend.GetItems()
			assert.Len(t, items, 1)
			assert.Contains(t, items, hashedCacheKey)
		})
	}
}

func TestLabelsQueryCache_ExplicitZeroStartOrEnd(t *testing.T) {
	const userID = "user-1"

	tests := map[string]struct {
		matcher                 string
		requests                []url.Values
		expectedDownstreamCalls int
	}{
		"end of 0, then no end": {
			matcher:                 `{job="test"}`,
			requests:                []url.Values{{"start": {"-3600"}, "end": {"0"}}, {"start": {"-3600"}}},
			expectedDownstreamCalls: 2,
		},
		"no end, then end of 0": {
			matcher:                 `{job="test"}`,
			requests:                []url.Values{{"start": {"-3600"}}, {"start": {"-3600"}, "end": {"0"}}},
			expectedDownstreamCalls: 2,
		},
		"end of 0, then no end, with a matcher that the optimizer removes": {
			matcher:                 `{__name__!="", job="test"}`,
			requests:                []url.Values{{"start": {"-3600"}, "end": {"0"}}, {"start": {"-3600"}}},
			expectedDownstreamCalls: 2,
		},
		"start of 0 in RFC3339, then no start": {
			matcher:                 `{job="test"}`,
			requests:                []url.Values{{"start": {"1970-01-01T00:00:00Z"}, "end": {"3600"}}, {"end": {"3600"}}},
			expectedDownstreamCalls: 2,
		},
		"same request twice, with a matcher that the optimizer removes": {
			matcher:                 `{__name__!="", job="test"}`,
			requests:                []url.Values{{"start": {"-3600"}}, {"start": {"-3600"}}},
			expectedDownstreamCalls: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodPost} {
				t.Run(method, func(t *testing.T) {
					downstreamCalls := 0
					downstream := RoundTripFunc(func(*http.Request) (*http.Response, error) {
						downstreamCalls++
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(strings.NewReader(`{"status":"success","data":[]}`)),
							Header:     http.Header{"Content-Type": []string{"application/json"}},
						}, nil
					})
					limits := multiTenantMockLimits{byTenant: map[string]mockLimits{
						userID: {resultsCacheTTLForLabelsQuery: time.Minute, labelsQueryOptimizerEnabled: true},
					}}
					codec := newTestCodec()
					logger := mimirtest.NewTestingLogger(t)
					// The optimizer runs before the cache, in the same order as the query-frontend.
					rt := newLabelsQueryCacheRoundTripper(cache.NewInstrumentedMockCache(), DefaultCacheKeyGenerator{codec: codec}, limits, downstream, logger, prometheus.NewPedanticRegistry())
					rt = newLabelsQueryOptimizer(codec, limits, rt, logger, prometheus.NewPedanticRegistry())

					for _, params := range tc.requests {
						params["match[]"] = []string{tc.matcher}
						var (
							req *http.Request
							err error
						)
						switch method {
						case http.MethodGet:
							req, err = http.NewRequest(method, "/prometheus/api/v1/labels?"+params.Encode(), nil)
							require.NoError(t, err)
						case http.MethodPost:
							req, err = http.NewRequest(method, "/prometheus/api/v1/labels", strings.NewReader(params.Encode()))
							require.NoError(t, err)
							req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
						}
						res, err := rt.RoundTrip(req.WithContext(user.InjectOrgID(t.Context(), userID)))
						require.NoError(t, err)
						require.Equal(t, http.StatusOK, res.StatusCode)
						_, err = io.ReadAll(res.Body)
						require.NoError(t, err)
					}

					assert.Equal(t, tc.expectedDownstreamCalls, downstreamCalls)
				})
			}
		})
	}
}

func TestDefaultCacheKeyGenerator_LabelValuesCacheKey(t *testing.T) {
	const labelName = "test"

	tests := map[string]struct {
		params                           url.Values
		expectedCacheKeyWithLabelName    string
		expectedCacheKeyWithoutLabelName string
	}{
		"no parameters provided": {
			expectedCacheKeyWithLabelName: strings.Join([]string{
				fmt.Sprintf("%d", v1.MinTime.UnixMilli()),
				fmt.Sprintf("%d", v1.MaxTime.UnixMilli()),
				labelName,
				"",
			}, string(stringParamSeparator)),
			expectedCacheKeyWithoutLabelName: strings.Join([]string{
				fmt.Sprintf("%d", v1.MinTime.UnixMilli()),
				fmt.Sprintf("%d", v1.MaxTime.UnixMilli()),
				"",
			}, string(stringParamSeparator)),
		},
		"only start parameter provided": {
			params: url.Values{
				"start": []string{"2023-07-05T01:00:00Z"},
			},
			expectedCacheKeyWithLabelName: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", v1.MaxTime.UnixMilli()),
				labelName,
				"",
			}, string(stringParamSeparator)),
			expectedCacheKeyWithoutLabelName: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", v1.MaxTime.UnixMilli()),
				"",
			}, string(stringParamSeparator)),
		},
		"only end parameter provided": {
			params: url.Values{
				"end": []string{"2023-07-05T07:00:00Z"},
			},
			expectedCacheKeyWithLabelName: strings.Join([]string{
				fmt.Sprintf("%d", v1.MinTime.UnixMilli()),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				labelName,
				"",
			}, string(stringParamSeparator)),
			expectedCacheKeyWithoutLabelName: strings.Join([]string{
				fmt.Sprintf("%d", v1.MinTime.UnixMilli()),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				"",
			}, string(stringParamSeparator)),
		},
		"only match[] parameter provided": {
			params: url.Values{
				"match[]": []string{`{second!="2",first="1"}`},
			},
			expectedCacheKeyWithLabelName: strings.Join([]string{
				fmt.Sprintf("%d", v1.MinTime.UnixMilli()),
				fmt.Sprintf("%d", v1.MaxTime.UnixMilli()),
				labelName,
				`{first="1",second!="2"}`,
			}, string(stringParamSeparator)),
			expectedCacheKeyWithoutLabelName: strings.Join([]string{
				fmt.Sprintf("%d", v1.MinTime.UnixMilli()),
				fmt.Sprintf("%d", v1.MaxTime.UnixMilli()),
				`{first="1",second!="2"}`,
			}, string(stringParamSeparator)),
		},
		"all parameters provided with mixed timestamp formats": {
			params: url.Values{
				"start":   []string{"2023-07-05T01:00:00Z"},
				"end":     []string{fmt.Sprintf("%d", mustParseTime("2023-07-05T07:00:00Z")/1000)},
				"match[]": []string{`{second!="2",first="1"}`, `{third="3"}`},
			},
			expectedCacheKeyWithLabelName: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				labelName,
				`{first="1",second!="2"},{third="3"}`,
			}, string(stringParamSeparator)),
			expectedCacheKeyWithoutLabelName: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				`{first="1",second!="2"},{third="3"}`,
			}, string(stringParamSeparator)),
		},
	}

	requestTypes := map[string]struct {
		requestPath                   string
		request                       *http.Request
		expectedCacheKeyPrefix        string
		expectedCacheKeyWithLabelName bool
	}{
		"label names API": {
			requestPath:                   "/api/v1/labels",
			expectedCacheKeyPrefix:        labelNamesQueryCachePrefix,
			expectedCacheKeyWithLabelName: false,
		},
		"label values API": {
			requestPath:                   fmt.Sprintf("/api/v1/label/%s/values", labelName),
			expectedCacheKeyPrefix:        labelValuesQueryCachePrefix,
			expectedCacheKeyWithLabelName: true,
		},
	}

	codec := newTestCodec()

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for requestTypeName, requestTypeData := range requestTypes {
				t.Run(requestTypeName, func(t *testing.T) {
					c := DefaultCacheKeyGenerator{codec: codec}
					requestURL, _ := url.Parse(requestTypeData.requestPath)
					requestURL.RawQuery = testData.params.Encode()
					request, err := http.NewRequest("GET", requestURL.String(), nil)
					require.NoError(t, err)

					actual, err := c.LabelValues(request)
					require.NoError(t, err)

					assert.Equal(t, requestTypeData.expectedCacheKeyPrefix, actual.CacheKeyPrefix)

					if requestTypeData.expectedCacheKeyWithLabelName {
						assert.Equal(t, testData.expectedCacheKeyWithLabelName, actual.CacheKey)
					} else {
						assert.Equal(t, testData.expectedCacheKeyWithoutLabelName, actual.CacheKey)
					}
				})
			}
		})
	}
}

func TestGenerateLabelsQueryRequestCacheKey(t *testing.T) {
	tests := map[string]struct {
		startTime        int64
		endTime          int64
		labelName        string
		matcherSets      [][]*labels.Matcher
		expectedCacheKey string
		limit            uint64
	}{
		"start and end time are aligned to 2h boundaries": {
			startTime: mustParseTime("2023-07-05T00:00:00Z"),
			endTime:   mustParseTime("2023-07-05T06:00:00Z"),
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				"",
			}, string(stringParamSeparator)),
		},
		"start and end time are not aligned to 2h boundaries": {
			startTime: mustParseTime("2023-07-05T01:23:00Z"),
			endTime:   mustParseTime("2023-07-05T06:23:00Z"),
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				"",
			}, string(stringParamSeparator)),
		},
		"start and end time match prometheus min/max time": {
			startTime: v1.MinTime.UnixMilli(),
			endTime:   v1.MaxTime.UnixMilli(),
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", v1.MinTime.UnixMilli()),
				fmt.Sprintf("%d", v1.MaxTime.UnixMilli()),
				"",
			}, string(stringParamSeparator)),
		},
		"single label matcher set": {
			startTime: mustParseTime("2023-07-05T00:00:00Z"),
			endTime:   mustParseTime("2023-07-05T06:00:00Z"),
			matcherSets: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
				},
			},
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				`{first="1",second!="2"}`,
			}, string(stringParamSeparator)),
		},
		"multiple label matcher sets": {
			startTime: mustParseTime("2023-07-05T00:00:00Z"),
			endTime:   mustParseTime("2023-07-05T06:00:00Z"),
			matcherSets: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
				}, {
					labels.MustNewMatcher(labels.MatchNotEqual, "first", "0"),
				},
			},
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				`{first="1",second!="2"},{first!="0"}`,
			}, string(stringParamSeparator)),
		},
		"multiple label matcher sets and label name": {
			startTime: mustParseTime("2023-07-05T00:00:00Z"),
			endTime:   mustParseTime("2023-07-05T06:00:00Z"),
			labelName: "test",
			matcherSets: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
				}, {
					labels.MustNewMatcher(labels.MatchNotEqual, "first", "0"),
				},
			},
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				"test",
				`{first="1",second!="2"},{first!="0"}`,
			}, string(stringParamSeparator)),
		},
		"multiple label matcher sets, label name, and limit": {
			startTime: mustParseTime("2023-07-05T00:00:00Z"),
			endTime:   mustParseTime("2023-07-05T06:00:00Z"),
			labelName: "test",
			matcherSets: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
				}, {
					labels.MustNewMatcher(labels.MatchNotEqual, "first", "0"),
				},
			},
			limit: 10,
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				"test",
				`{first="1",second!="2"},{first!="0"}`,
				"10",
			}, string(stringParamSeparator)),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedCacheKey, generateLabelsQueryRequestCacheKey(testData.startTime, testData.endTime, testData.labelName, testData.matcherSets, testData.limit))
		})
	}
}

func TestParseRequestMatchersParam(t *testing.T) {
	const paramName = "match[]"

	tests := map[string]struct {
		input    url.Values
		expected [][]*labels.Matcher
	}{
		"single label matcher set with unique label names": {
			input: url.Values{
				paramName: []string{`{second!="2",first="1"}`},
			},
			expected: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
				},
			},
		},
		"single label matcher set with duplicated label names": {
			input: url.Values{
				paramName: []string{`{second!="2",second!="1",first="1"}`},
			},
			expected: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
				},
			},
		},
		"multiple label matcher sets with the same number of matchers each": {
			input: url.Values{
				paramName: []string{`{second!="2",first="1"}`, `{first="1",second!="1"}`},
			},
			expected: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "1"),
				}, {
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
				},
			},
		},
		"multiple label matcher sets with a different number of matchers each": {
			input: url.Values{
				paramName: []string{`{second!="2",first="1"}`, `{first="1",second!="2",third="3"}`},
			},
			expected: [][]*labels.Matcher{
				{
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
				}, {
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
					labels.MustNewMatcher(labels.MatchEqual, "third", "3"),
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Run("GET request", func(t *testing.T) {
				req, err := http.NewRequest("GET", "http://localhost?"+testData.input.Encode(), nil)
				require.NoError(t, err)
				require.NoError(t, req.ParseForm())

				actual, err := parseRequestMatchersParam(req.Form, paramName)
				require.NoError(t, err)

				assert.Equal(t, testData.expected, actual)
			})

			t.Run("POST request", func(t *testing.T) {
				req, err := http.NewRequest("POST", "http://localhost/", strings.NewReader(testData.input.Encode()))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				require.NoError(t, req.ParseForm())

				actual, err := parseRequestMatchersParam(req.Form, "match[]")
				require.NoError(t, err)

				assert.Equal(t, testData.expected, actual)
			})
		})
	}
}

func mustParseTime(input string) int64 {
	parsed, err := time.Parse(time.RFC3339, input)
	if err != nil {
		panic(err)
	}
	return parsed.UnixMilli()
}
