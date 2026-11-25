// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/api/handlers_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/util/propagation"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIndexPageContent(t *testing.T) {
	c := newIndexPageContent("")
	c.AddLinks(defaultWeight, "Some group", []IndexPageLink{
		{Desc: "Some link", Path: "store-gateway/ring"},
		{Dangerous: true, Desc: "Boom!", Path: "store-gateway/boom"},
	})

	h := indexHandler(c)

	req := httptest.NewRequest("GET", "/", nil)
	resp := httptest.NewRecorder()

	h.ServeHTTP(resp, req)

	require.Equal(t, 200, resp.Code)
	require.True(t, strings.Contains(resp.Body.String(), "Some group"))
	require.True(t, strings.Contains(resp.Body.String(), "Some link"))
	require.True(t, strings.Contains(resp.Body.String(), "Dangerous"))
	require.True(t, strings.Contains(resp.Body.String(), "Boom!"))
	require.True(t, strings.Contains(resp.Body.String(), "Dangerous"))
	require.True(t, strings.Contains(resp.Body.String(), "store-gateway/ring"))
	require.True(t, strings.Contains(resp.Body.String(), "store-gateway/boom"))
	require.False(t, strings.Contains(resp.Body.String(), "compactor/ring"))
}

func TestResolveBaseURL(t *testing.T) {
	result := resolveBaseURL("/mimir", "/config")
	require.Equal(t, "/mimir/config", result)

	result = resolveBaseURL("/mimir/", "/config")
	require.Equal(t, "/mimir/config", result)

	result = resolveBaseURL("/mimir", "runtime_config")
	require.Equal(t, "runtime_config", result)
}

type diffConfigMock struct {
	MyInt          int          `yaml:"my_int"`
	MyFloat        float64      `yaml:"my_float"`
	MySlice        []string     `yaml:"my_slice"`
	IgnoredField   func() error `yaml:"-"`
	MyNestedStruct struct {
		MyString      string   `yaml:"my_string"`
		MyBool        bool     `yaml:"my_bool"`
		MyEmptyStruct struct{} `yaml:"my_empty_struct"`
	} `yaml:"my_nested_struct"`
}

func newDefaultDiffConfigMock() *diffConfigMock {
	c := &diffConfigMock{
		MyInt:        666,
		MyFloat:      6.66,
		MySlice:      []string{"value1", "value2"},
		IgnoredField: func() error { return nil },
	}
	c.MyNestedStruct.MyString = "string1"
	return c
}

func TestConfigDiffHandler(t *testing.T) {
	for _, tc := range []struct {
		name               string
		expectedStatusCode int
		expectedBody       string
		actualConfig       func() interface{}
	}{
		{
			name:               "no config parameters overridden",
			expectedStatusCode: 200,
			expectedBody:       "{}\n",
		},
		{
			name: "slice changed",
			actualConfig: func() interface{} {
				c := newDefaultDiffConfigMock()
				c.MySlice = append(c.MySlice, "value3")
				return c
			},
			expectedStatusCode: 200,
			expectedBody: "my_slice:\n" +
				"    - value1\n" +
				"    - value2\n" +
				"    - value3\n",
		},
		{
			name: "string in nested struct changed",
			actualConfig: func() interface{} {
				c := newDefaultDiffConfigMock()
				c.MyNestedStruct.MyString = "string2"
				return c
			},
			expectedStatusCode: 200,
			expectedBody: "my_nested_struct:\n" +
				"    my_string: string2\n",
		},
		{
			name: "bool in nested struct changed",
			actualConfig: func() interface{} {
				c := newDefaultDiffConfigMock()
				c.MyNestedStruct.MyBool = true
				return c
			},
			expectedStatusCode: 200,
			expectedBody: "my_nested_struct:\n" +
				"    my_bool: true\n",
		},
		{
			name: "test invalid input",
			actualConfig: func() interface{} {
				c := "x"
				return &c
			},
			expectedStatusCode: 500,
			expectedBody: "yaml: unmarshal errors:\n" +
				"  line 1: cannot unmarshal !!str `x` into map[string]interface {}\n",
		},
	} {
		defaultCfg := newDefaultDiffConfigMock()
		t.Run(tc.name, func(t *testing.T) {

			var actualCfg interface{}
			if tc.actualConfig != nil {
				actualCfg = tc.actualConfig()
			} else {
				actualCfg = newDefaultDiffConfigMock()
			}

			req := httptest.NewRequest("GET", "http://localhost/config?mode=diff", nil)
			w := httptest.NewRecorder()

			h := DefaultConfigHandler(actualCfg, defaultCfg)
			h(w, req)
			resp := w.Result()
			assert.Equal(t, tc.expectedStatusCode, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedBody, string(body))
		})
	}
}

func TestConfigOverrideHandler(t *testing.T) {
	cfg := &Config{
		CustomConfigHandler: func(_ interface{}, _ interface{}) http.HandlerFunc {
			return func(w http.ResponseWriter, _ *http.Request) {
				_, err := w.Write([]byte("config"))
				assert.NoError(t, err)
			}
		},
	}

	req := httptest.NewRequest("GET", "http://localhost/config", nil)
	w := httptest.NewRecorder()

	h := cfg.configHandler(
		struct{ name string }{name: "actual"},
		struct{ name string }{name: "default"},
	)
	h(w, req)
	resp := w.Result()
	assert.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, []byte("config"), body)
}

type metadataRangeTestQueryable struct {
	storage.SampleAndChunkQueryable
	called     bool
	mint, maxt int64
}

func (q *metadataRangeTestQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	q.called, q.mint, q.maxt = true, mint, maxt
	return &storage.MockQuerier{SelectMockFunction: func(bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet {
		return storage.EmptySeriesSet()
	}}, nil
}

func TestQuerierHandler_MetadataDefaultTimeRange(t *testing.T) {
	for _, tc := range []struct{ method, endpoint string }{
		{http.MethodGet, "labels"},
		{http.MethodPost, "labels"},
		{http.MethodGet, "label/job/values"},
		{http.MethodGet, "series"},
		{http.MethodPost, "series"},
	} {
		for _, limit := range []time.Duration{0, time.Hour} {
			t.Run(fmt.Sprintf("%s %s, limit %s", tc.method, tc.endpoint, limit), func(t *testing.T) {
				var cfg querier.Config
				flagext.DefaultValues(&cfg)
				limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
					defaults.MetadataDefaultTimeRange = model.Duration(limit)
				})
				queryable := &metadataRangeTestQueryable{}
				reg := prometheus.NewRegistry()
				handler := NewQuerierHandler(Config{ServerPrefix: "/mimir", PrometheusHTTPPrefix: "/prometheus"}, cfg, queryable, nil, nil, nil, nil, querier.NewRequestMetrics(reg), reg, log.NewNopLogger(), limits, &propagation.NoopExtractor{})

				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, costTestRequest(t, tc.method, tc.endpoint, url.Values{"match[]": {`{__name__="up"}`}, "end": {"10800"}}))
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var response struct {
					Status   string   `json:"status"`
					Warnings []string `json:"warnings"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
				require.Equal(t, "success", response.Status)
				require.True(t, queryable.called)
				require.Equal(t, (3 * time.Hour).Milliseconds(), queryable.maxt)
				if limit == 0 {
					require.Equal(t, v1.MinTime.UnixMilli(), queryable.mint)
					require.Empty(t, response.Warnings)
				} else {
					require.Equal(t, (2 * time.Hour).Milliseconds(), queryable.mint)
					require.Equal(t, []string{"start or end not given, applied a default: start=1970-01-01T02:00:00Z, end=1970-01-01T03:00:00Z"}, response.Warnings)
				}
			})
		}
	}
}
