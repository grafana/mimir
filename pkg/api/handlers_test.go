// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/api/handlers_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
)

func TestNewQuerierHandler_CardinalityEstimateRouteRunsMiddleware(t *testing.T) {
	offsets := querierapi.EncodeOffsetsV1(map[int32]int64{0: 1, 1: 2})
	var (
		gotLevel   string
		gotOffsets querierapi.EncodedOffsets
	)
	estimate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotLevel, _ = querierapi.ReadConsistencyLevelFromContext(r.Context())
		gotOffsets, _ = querierapi.ReadConsistencyEncodedOffsetsFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	reg := prometheus.NewPedanticRegistry()
	metrics := querier.NewRequestMetrics(reg)
	newHandler := func(estimate http.Handler) http.Handler {
		return NewQuerierHandler(Config{PrometheusHTTPPrefix: "/prometheus"}, querier.Config{}, nil, nil, nil, nil, nil, metrics, nil, log.NewNopLogger(), nil, &querierapi.ConsistencyExtractor{}, estimate)
	}
	h := newHandler(estimate)

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		gotLevel, gotOffsets = "", ""
		req := httptest.NewRequest(method, "/prometheus/api/v1/cardinality/estimate?start=0&end=1", nil)
		req = req.WithContext(user.InjectOrgID(req.Context(), "user-1"))
		req.Header.Set(querierapi.ReadConsistencyHeader, querierapi.ReadConsistencyStrong)
		req.Header.Set(querierapi.ReadConsistencyOffsetsHeader, string(offsets))
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)

		require.Equal(t, http.StatusOK, resp.Code, method)
		assert.Equal(t, querierapi.ReadConsistencyStrong, gotLevel, method)
		assert.Equal(t, offsets, gotOffsets, method)
	}
	assert.Equal(t, 2, testutil.CollectAndCount(reg, "cortex_querier_request_duration_seconds"), "the route is instrumented")

	// Without a handler the route is not registered.
	resp := httptest.NewRecorder()
	newHandler(nil).ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/cardinality/estimate?start=0&end=1", nil))
	assert.Equal(t, http.StatusNotFound, resp.Code)
}

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
