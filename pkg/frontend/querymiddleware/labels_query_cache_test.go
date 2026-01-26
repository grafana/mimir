// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

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
		// usesDefaultMetricMatch indicates that the request type includes
		// the default metric_match value ("target_info") in the cache key.
		usesDefaultMetricMatch bool
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
		"info labels API": {
			requestPath:                   "/api/v1/info_labels",
			expectedCacheKeyPrefix:        infoLabelsQueryCachePrefix,
			expectedCacheKeyWithLabelName: false,
			usesDefaultMetricMatch:        true,
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

					var expectedCacheKey string
					if requestTypeData.expectedCacheKeyWithLabelName {
						expectedCacheKey = testData.expectedCacheKeyWithLabelName
					} else {
						expectedCacheKey = testData.expectedCacheKeyWithoutLabelName
					}

					// Info labels requests include the default metric_match value in the cache key.
					if requestTypeData.usesDefaultMetricMatch {
						expectedCacheKey = expectedCacheKey + string(stringParamSeparator) + "target_info"
					}

					assert.Equal(t, expectedCacheKey, actual.CacheKey)
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
		metricMatch      string
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
		"with metric_match for info_labels": {
			startTime:   mustParseTime("2023-07-05T00:00:00Z"),
			endTime:     mustParseTime("2023-07-05T06:00:00Z"),
			metricMatch: "target_info",
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				"",
				"target_info",
			}, string(stringParamSeparator)),
		},
		"with metric_match and limit": {
			startTime:   mustParseTime("2023-07-05T00:00:00Z"),
			endTime:     mustParseTime("2023-07-05T06:00:00Z"),
			limit:       5,
			metricMatch: "build_info",
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				"",
				"5",
				"build_info",
			}, string(stringParamSeparator)),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedCacheKey, generateLabelsQueryRequestCacheKey(testData.startTime, testData.endTime, testData.labelName, testData.matcherSets, testData.limit, testData.metricMatch))
		})
	}
}

func TestInfoLabelsCacheKeyIsolation(t *testing.T) {
	codec := newTestCodec()
	c := DefaultCacheKeyGenerator{codec: codec}

	// Create requests with the same parameters but different endpoints/metric_match values.
	labelNamesReq, _ := http.NewRequest("GET", "/api/v1/labels?start=1000&end=2000", nil)
	infoLabelsDefaultReq, _ := http.NewRequest("GET", "/api/v1/info_labels?start=1000&end=2000", nil)
	infoLabelsTargetInfoReq, _ := http.NewRequest("GET", "/api/v1/info_labels?start=1000&end=2000&metric_match=target_info", nil)
	infoLabelsBuildInfoReq, _ := http.NewRequest("GET", "/api/v1/info_labels?start=1000&end=2000&metric_match=build_info", nil)

	labelNamesKey, err := c.LabelValues(labelNamesReq)
	require.NoError(t, err)

	infoLabelsDefaultKey, err := c.LabelValues(infoLabelsDefaultReq)
	require.NoError(t, err)

	infoLabelsTargetInfoKey, err := c.LabelValues(infoLabelsTargetInfoReq)
	require.NoError(t, err)

	infoLabelsBuildInfoKey, err := c.LabelValues(infoLabelsBuildInfoReq)
	require.NoError(t, err)

	// Verify different prefixes for label_names vs info_labels.
	assert.Equal(t, labelNamesQueryCachePrefix, labelNamesKey.CacheKeyPrefix, "label names should use ln: prefix")
	assert.Equal(t, infoLabelsQueryCachePrefix, infoLabelsDefaultKey.CacheKeyPrefix, "info labels should use il: prefix")
	assert.Equal(t, infoLabelsQueryCachePrefix, infoLabelsTargetInfoKey.CacheKeyPrefix, "info labels should use il: prefix")
	assert.Equal(t, infoLabelsQueryCachePrefix, infoLabelsBuildInfoKey.CacheKeyPrefix, "info labels should use il: prefix")

	// Verify label_names and info_labels have different cache keys even with same start/end.
	assert.NotEqual(t, labelNamesKey.CacheKey, infoLabelsDefaultKey.CacheKey,
		"label_names and info_labels should have different cache keys")

	// Verify info_labels with default metric_match equals explicit target_info.
	assert.Equal(t, infoLabelsDefaultKey.CacheKey, infoLabelsTargetInfoKey.CacheKey,
		"info_labels with default and explicit target_info should have same cache key")

	// Verify different metric_match values produce different cache keys.
	assert.NotEqual(t, infoLabelsTargetInfoKey.CacheKey, infoLabelsBuildInfoKey.CacheKey,
		"info_labels with different metric_match values should have different cache keys")
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
