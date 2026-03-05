// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceAttributesQueryCache_RoundTrip(t *testing.T) {
	testGenericQueryCacheRoundTrip(t, newResourceAttributesQueryCacheRoundTripper, queryTypeResourceAttributes, map[string]testGenericQueryCacheRequestType{
		"resource attributes request": {
			reqPath: "/prometheus/api/v1/resources",
			reqData: url.Values{
				"start":   []string{"2023-07-05T01:00:00Z"},
				"end":     []string{"2023-07-05T08:00:00Z"},
				"match[]": []string{`{job="test_1"}`, `{job!="test_2"}`},
			},
			cacheKey:       fmt.Sprintf("user-1:%d\x00%d\x00{job!=\"test_2\"},{job=\"test_1\"}", mustParseTime("2023-07-05T00:00:00Z"), mustParseTime("2023-07-05T08:00:00Z")),
			hashedCacheKey: resourceAttributesQueryCachePrefix + hashCacheKey(fmt.Sprintf("user-1:%d\x00%d\x00{job!=\"test_2\"},{job=\"test_1\"}", mustParseTime("2023-07-05T00:00:00Z"), mustParseTime("2023-07-05T08:00:00Z"))),
		},
		"resource attributes series request": {
			reqPath: "/prometheus/api/v1/resources/series",
			reqData: url.Values{
				"start":         []string{"2023-07-05T01:00:00Z"},
				"end":           []string{"2023-07-05T08:00:00Z"},
				"resource.attr": []string{"service.name:test", "cloud.region:us-west-2"},
			},
			cacheKey:       fmt.Sprintf("user-1:%d\x00%d\x00cloud.region:us-west-2\x00service.name:test", mustParseTime("2023-07-05T00:00:00Z"), mustParseTime("2023-07-05T08:00:00Z")),
			hashedCacheKey: resourceAttributesSeriesQueryCachePrefix + hashCacheKey(fmt.Sprintf("user-1:%d\x00%d\x00cloud.region:us-west-2\x00service.name:test", mustParseTime("2023-07-05T00:00:00Z"), mustParseTime("2023-07-05T08:00:00Z"))),
		},
	})
}

func TestDefaultCacheKeyGenerator_ResourceAttributes(t *testing.T) {
	tests := map[string]struct {
		path               string
		params             url.Values
		expectedPrefix     string
		expectedCacheKey   string
		expectedErrContain string
	}{
		"/api/v1/resources with matchers": {
			path: "/api/v1/resources",
			params: url.Values{
				"start":   []string{"2023-07-05T01:00:00Z"},
				"end":     []string{"2023-07-05T08:00:00Z"},
				"match[]": []string{`{job="test_1"}`, `{job!="test_2"}`},
			},
			expectedPrefix: resourceAttributesQueryCachePrefix,
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				`{job!="test_2"},{job="test_1"}`,
			}, string(stringParamSeparator)),
		},
		"/api/v1/resources with matchers and limit": {
			path: "/api/v1/resources",
			params: url.Values{
				"start":   []string{"2023-07-05T01:00:00Z"},
				"end":     []string{"2023-07-05T08:00:00Z"},
				"match[]": []string{`{job="test_1"}`},
				"limit":   []string{"100"},
			},
			expectedPrefix: resourceAttributesQueryCachePrefix,
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				`{job="test_1"}`,
				"100",
			}, string(stringParamSeparator)),
		},
		"/api/v1/resources/series with filters": {
			path: "/api/v1/resources/series",
			params: url.Values{
				"start":         []string{"2023-07-05T01:00:00Z"},
				"end":           []string{"2023-07-05T08:00:00Z"},
				"resource.attr": []string{"service.name:test", "cloud.region:us-west-2"},
			},
			expectedPrefix: resourceAttributesSeriesQueryCachePrefix,
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				"cloud.region:us-west-2",
				"service.name:test",
			}, string(stringParamSeparator)),
		},
		"/api/v1/resources/series with filters and limit": {
			path: "/api/v1/resources/series",
			params: url.Values{
				"start":         []string{"2023-07-05T01:00:00Z"},
				"end":           []string{"2023-07-05T08:00:00Z"},
				"resource.attr": []string{"service.name:test"},
				"limit":         []string{"50"},
			},
			expectedPrefix: resourceAttributesSeriesQueryCachePrefix,
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				"service.name:test",
				"50",
			}, string(stringParamSeparator)),
		},
		"unknown endpoint": {
			path:               "/api/v1/unknown",
			params:             url.Values{},
			expectedErrContain: "unknown resource attributes API endpoint",
		},
	}

	codec := newTestCodec()

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			c := DefaultCacheKeyGenerator{codec: codec}
			requestURL, _ := url.Parse(testData.path)
			requestURL.RawQuery = testData.params.Encode()
			request, err := http.NewRequest("GET", requestURL.String(), nil)
			require.NoError(t, err)

			actual, err := c.ResourceAttributes(request)
			if testData.expectedErrContain != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testData.expectedErrContain)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, testData.expectedPrefix, actual.CacheKeyPrefix)
			assert.Equal(t, testData.expectedCacheKey, actual.CacheKey)
		})
	}
}

func TestGenerateResourceAttributesQueryCacheKey(t *testing.T) {
	tests := map[string]struct {
		path             string
		values           url.Values
		expectedCacheKey string
	}{
		"resource attributes with time alignment": {
			path: "/api/v1/resources",
			values: url.Values{
				"start":   []string{"2023-07-05T01:23:00Z"},
				"end":     []string{"2023-07-05T06:23:00Z"},
				"match[]": []string{`{job="test"}`},
			},
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T08:00:00Z")),
				`{job="test"}`,
			}, string(stringParamSeparator)),
		},
		"resource attributes series with sorted filters": {
			path: "/api/v1/resources/series",
			values: url.Values{
				"start":         []string{"2023-07-05T00:00:00Z"},
				"end":           []string{"2023-07-05T06:00:00Z"},
				"resource.attr": []string{"z.key:val", "a.key:val"},
			},
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				"a.key:val",
				"z.key:val",
			}, string(stringParamSeparator)),
		},
		"resource attributes with sorted matchers": {
			path: "/api/v1/resources",
			values: url.Values{
				"start":   []string{"2023-07-05T00:00:00Z"},
				"end":     []string{"2023-07-05T06:00:00Z"},
				"match[]": []string{`{z="2",a="1"}`},
			},
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				`{a="1",z="2"}`,
			}, string(stringParamSeparator)),
		},
		"different paths produce different keys": {
			path: "/api/v1/resources/series",
			values: url.Values{
				"start":         []string{"2023-07-05T00:00:00Z"},
				"end":           []string{"2023-07-05T06:00:00Z"},
				"resource.attr": []string{"key:val"},
			},
			expectedCacheKey: strings.Join([]string{
				fmt.Sprintf("%d", mustParseTime("2023-07-05T00:00:00Z")),
				fmt.Sprintf("%d", mustParseTime("2023-07-05T06:00:00Z")),
				"key:val",
			}, string(stringParamSeparator)),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual, err := generateResourceAttributesQueryCacheKey(testData.path, testData.values)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedCacheKey, actual)
		})
	}
}
