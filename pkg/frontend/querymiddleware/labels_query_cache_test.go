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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util"
)

func TestLabelsQueryCache_RoundTrip(t *testing.T) {
	testGenericQueryCacheRoundTrip(t, newLabelsQueryCacheRoundTripper, "label_names_and_values", map[string]testGenericQueryCacheRequestType{
		"label names request": {
			url:            mustParseURL(t, `/prometheus/api/v1/labels?start=2023-07-05T01:00:00Z&end=2023-07-05T08:00:00Z&match[]={job="test_1"}&match[]={job!="test_2"}`),
			cacheKey:       "user-1:1688515200000\x001688544000000\x002\x00job!=\"test_2\"\x00job=\"test_1\"",
			hashedCacheKey: labelNamesQueryCachePrefix + cacheHashKey("user-1:1688515200000\x001688544000000\x002\x00job!=\"test_2\"\x00job=\"test_1\""),
		},
		"label values request": {
			url:            mustParseURL(t, `/prometheus/api/v1/label/test/values?start=2023-07-05T01:00:00Z&end=2023-07-05T08:00:00Z&match[]={job="test_1"}&match[]={job!="test_2"}`),
			cacheKey:       "user-1:1688515200000\x001688544000000\x00test\x002\x00job!=\"test_2\"\x00job=\"test_1\"",
			hashedCacheKey: labelValuesQueryCachePrefix + cacheHashKey("user-1:1688515200000\x001688544000000\x00test\x002\x00job!=\"test_2\"\x00job=\"test_1\""),
		},
	})
}

func TestLabelsQueryCache_parseRequest(t *testing.T) {
	const labelName = "test"

	tests := map[string]struct {
		params                           url.Values
		expectedCacheKeyWithLabelName    string
		expectedCacheKeyWithoutLabelName string
	}{
		"no parameters provided": {
			expectedCacheKeyWithLabelName: strings.Join([]string{
				fmt.Sprintf("%d", util.PrometheusMinTime.UnixMilli()),
				fmt.Sprintf("%d", util.PrometheusMaxTime.UnixMilli()),
				labelName,
				"0",
			}, string(stringParamSeparator)),
			expectedCacheKeyWithoutLabelName: strings.Join([]string{
				fmt.Sprintf("%d", util.PrometheusMinTime.UnixMilli()),
				fmt.Sprintf("%d", util.PrometheusMaxTime.UnixMilli()),
				"0",
			}, string(stringParamSeparator)),
		},
		"only start parameter provided": {
			params: url.Values{
				"start": []string{"2023-07-05T01:00:00Z"},
			},
			expectedCacheKeyWithLabelName: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", util.PrometheusMaxTime.UnixMilli()),
				labelName,
				"0",
			}, string(stringParamSeparator)),
			expectedCacheKeyWithoutLabelName: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", util.PrometheusMaxTime.UnixMilli()),
				"0",
			}, string(stringParamSeparator)),
		},
		"only end parameter provided": {
			params: url.Values{
				"end": []string{"2023-07-05T07:00:00Z"},
			},
			expectedCacheKeyWithLabelName: strings.Join([]string{
				fmt.Sprintf("%d", util.PrometheusMinTime.UnixMilli()),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				labelName,
				"0",
			}, string(stringParamSeparator)),
			expectedCacheKeyWithoutLabelName: strings.Join([]string{
				fmt.Sprintf("%d", util.PrometheusMinTime.UnixMilli()),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				"0",
			}, string(stringParamSeparator)),
		},
		"only match[] parameter provided": {
			params: url.Values{
				"match[]": []string{`{second!="2",first="1"}`},
			},
			expectedCacheKeyWithLabelName: strings.Join([]string{
				fmt.Sprintf("%d", util.PrometheusMinTime.UnixMilli()),
				fmt.Sprintf("%d", util.PrometheusMaxTime.UnixMilli()),
				labelName,
				"1",
				strings.Join([]string{`first="1"`, `second!="2"`}, string(stringValueSeparator)),
			}, string(stringParamSeparator)),
			expectedCacheKeyWithoutLabelName: strings.Join([]string{
				fmt.Sprintf("%d", util.PrometheusMinTime.UnixMilli()),
				fmt.Sprintf("%d", util.PrometheusMaxTime.UnixMilli()),
				"1",
				strings.Join([]string{`first="1"`, `second!="2"`}, string(stringValueSeparator)),
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
				"2",
				strings.Join([]string{`first="1"`, `second!="2"`}, string(stringValueSeparator)),
				`third="3"`,
			}, string(stringParamSeparator)),
			expectedCacheKeyWithoutLabelName: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				"2",
				strings.Join([]string{`first="1"`, `second!="2"`}, string(stringValueSeparator)),
				`third="3"`,
			}, string(stringParamSeparator)),
		},
	}

	requestTypes := map[string]struct {
		requestPath                   string
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

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for requestTypeName, requestTypeData := range requestTypes {
				t.Run(requestTypeName, func(t *testing.T) {
					req, err := http.NewRequest("GET", "http://localhost"+requestTypeData.requestPath+"?"+testData.params.Encode(), nil)
					require.NoError(t, err)

					c := &labelsQueryCache{}
					actual, err := c.parseRequest(req)
					require.NoError(t, err)

					assert.Equal(t, requestTypeData.expectedCacheKeyPrefix, actual.cacheKeyPrefix)

					if requestTypeData.expectedCacheKeyWithLabelName {
						assert.Equal(t, testData.expectedCacheKeyWithLabelName, actual.cacheKey)
					} else {
						assert.Equal(t, testData.expectedCacheKeyWithoutLabelName, actual.cacheKey)
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
	}{
		"start and end time are aligned to 2h boundaries": {
			startTime: mustParseTime("2023-07-05T00:00:00Z"),
			endTime:   mustParseTime("2023-07-05T06:00:00Z"),
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				"0",
			}, string(stringParamSeparator)),
		},
		"start and end time are not aligned to 2h boundaries": {
			startTime: mustParseTime("2023-07-05T01:23:00Z"),
			endTime:   mustParseTime("2023-07-05T06:23:00Z"),
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				"0",
			}, string(stringParamSeparator)),
		},
		"start and end time match prometheus min/max time": {
			startTime: util.PrometheusMinTime.UnixMilli(),
			endTime:   util.PrometheusMaxTime.UnixMilli(),
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", util.PrometheusMinTime.UnixMilli()),
				fmt.Sprintf("%d", util.PrometheusMaxTime.UnixMilli()),
				"0",
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
				"1",
				strings.Join([]string{`first="1"`, `second!="2"`}, string(stringValueSeparator)),
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
				"2",
				strings.Join([]string{`first="1"`, `second!="2"`}, string(stringValueSeparator)),
				`first!="0"`,
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
				"2",
				strings.Join([]string{`first="1"`, `second!="2"`}, string(stringValueSeparator)),
				`first!="0"`,
			}, string(stringParamSeparator)),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedCacheKey, generateLabelsQueryRequestCacheKey(testData.startTime, testData.endTime, testData.labelName, testData.matcherSets))
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

				actual, err := parseRequestMatchersParam(req, paramName)
				require.NoError(t, err)

				assert.Equal(t, testData.expected, actual)
			})

			t.Run("POST request", func(t *testing.T) {
				req, err := http.NewRequest("POST", "http://localhost/", strings.NewReader(testData.input.Encode()))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				require.NoError(t, req.ParseForm())

				actual, err := parseRequestMatchersParam(req, "match[]")
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
