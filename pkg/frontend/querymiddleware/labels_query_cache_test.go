// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabelsQueryCache_RoundTrip(t *testing.T) {
	testGenericQueryCacheRoundTrip(t, newLabelsQueryCacheRoundTripper, "label_names_and_values", map[string]testGenericQueryCacheRequestType{
		"label names request": {
			reqPath:        "/prometheus/api/v1/labels",
			reqData:        url.Values{"start": []string{"2023-07-05T01:00:00Z"}, "end": []string{"2023-07-05T08:00:00Z"}, "match[]": []string{`{job="test_1"}`, `{job!="test_2"}`}},
			cacheKey:       "user-1:1688515200000\x001688544000000\x00{job!=\"test_2\"},{job=\"test_1\"}",
			hashedCacheKey: labelNamesQueryCachePrefix + cacheHashKey("user-1:1688515200000\x001688544000000\x00{job!=\"test_2\"},{job=\"test_1\"}"),
		},
		"label values request": {
			reqPath:        "/prometheus/api/v1/label/test/values",
			reqData:        url.Values{"start": []string{"2023-07-05T01:00:00Z"}, "end": []string{"2023-07-05T08:00:00Z"}, "match[]": []string{`{job="test_1"}`, `{job!="test_2"}`}},
			cacheKey:       "user-1:1688515200000\x001688544000000\x00test\x00{job!=\"test_2\"},{job=\"test_1\"}",
			hashedCacheKey: labelValuesQueryCachePrefix + cacheHashKey("user-1:1688515200000\x001688544000000\x00test\x00{job!=\"test_2\"},{job=\"test_1\"}"),
		},
	})
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

	reg := prometheus.NewPedanticRegistry()
	codec := NewPrometheusCodec(reg, 0*time.Minute, formatJSON, nil)

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
