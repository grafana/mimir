// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimirtest "github.com/grafana/mimir/pkg/util/test"
)

func TestLabelsQueryOptimizer_RoundTrip(t *testing.T) {
	const userID = "user-1"

	tests := map[string]struct {
		optimizerEnabled         bool
		reqPath                  string
		reqData                  url.Values
		expectedDownstreamParams url.Values
	}{
		"should optimize labels request when enabled": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="prometheus"}`}},
		},
		"should optimize multiple matchers": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`, `{__name__!="", instance="localhost"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="prometheus"}`, `{instance="localhost"}`}},
		},
		"should handle mixed optimizable and non-optimizable matchers": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{job="test"}`, `{__name__!="", env="prod"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="test"}`, `{env="prod"}`}},
		},
		"should pass through when optimization disabled": {
			optimizerEnabled:         false,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
		},
		"should pass through when no optimization needed": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="prometheus"}`}},
		},
		"should pass through non-labels requests": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/query",
			reqData:                  url.Values{"query": []string{"up"}},
			expectedDownstreamParams: url.Values{"query": []string{"up"}},
		},
		"should pass through when no matchers": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{},
			expectedDownstreamParams: url.Values{},
		},
		"should pass through when malformed matchers": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{"{invalid syntax"}},
			expectedDownstreamParams: url.Values{"match[]": []string{"{invalid syntax"}},
		},
		// TODO another path
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, reqMethod := range []string{http.MethodGet, http.MethodPost} {
				t.Run(reqMethod, func(t *testing.T) {
					// Mock the limits.
					limits := multiTenantMockLimits{
						byTenant: map[string]mockLimits{
							userID: {
								labelsQueryOptimizerEnabled: testData.optimizerEnabled,
							},
						},
					}

					var (
						req                 *http.Request
						downstreamCalled    = false
						downstreamReqParams url.Values
						err                 error
					)

					// Mock the downstream and capture the request.
					downstream := RoundTripFunc(func(req *http.Request) (*http.Response, error) {
						downstreamCalled = true

						// Parse the request form to capture parameters.
						require.NoError(t, req.ParseForm())
						downstreamReqParams = req.Form

						return &http.Response{StatusCode: http.StatusOK}, nil
					})

					// Create the request.
					switch reqMethod {
					case http.MethodGet:
						req, err = http.NewRequest(reqMethod, testData.reqPath+"?"+testData.reqData.Encode(), nil)
						require.NoError(t, err)
					case http.MethodPost:
						req, err = http.NewRequest(reqMethod, testData.reqPath, strings.NewReader(testData.reqData.Encode()))
						req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
						require.NoError(t, err)
					default:
						t.Fatalf("unsupported HTTP method %q", reqMethod)
					}

					// Inject the tenant ID in the request
					req = req.WithContext(user.InjectOrgID(context.Background(), userID))

					// Create the labels query optimizer
					codec := NewPrometheusCodec(prometheus.NewRegistry(), 0*time.Minute, formatJSON, nil)
					optimizer := newLabelsQueryOptimizer(codec, limits, downstream, mimirtest.NewTestingLogger(t))

					// Execute the request
					_, err = optimizer.RoundTrip(req)
					require.NoError(t, err)

					// Ensure the downstream has been called with the expected params.
					require.True(t, downstreamCalled)
					require.Equal(t, testData.expectedDownstreamParams, downstreamReqParams)
				})
			}
		})
	}
}

func TestOptimizeLabelNamesRequestMatchers(t *testing.T) {
	tests := map[string]struct {
		inputMatchers     []string
		expectedMatchers  []string
		expectedOptimized bool
		expectedError     bool
	}{
		"empty matchers": {
			inputMatchers:     []string{},
			expectedMatchers:  []string{},
			expectedOptimized: false,
			expectedError:     false,
		},
		`single __name__!="" matcher`: {
			inputMatchers:     []string{`{__name__!=""}`},
			expectedMatchers:  []string{},
			expectedOptimized: true,
			expectedError:     false,
		},
		`single __name__!="" matcher with job label`: {
			inputMatchers:     []string{`{__name__!="", job="prometheus"}`},
			expectedMatchers:  []string{`{job="prometheus"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		`multiple labels with __name__!=""`: {
			inputMatchers:     []string{`{__name__!="", job="prometheus", instance="localhost:9090"}`},
			expectedMatchers:  []string{`{job="prometheus",instance="localhost:9090"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		`no __name__!="" matcher`: {
			inputMatchers:     []string{`{job="prometheus"}`},
			expectedMatchers:  []string{`{job="prometheus"}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		"__name__ equal matcher": {
			inputMatchers:     []string{`{__name__="up"}`},
			expectedMatchers:  []string{`{__name__="up"}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		"__name__ regex matchers": {
			inputMatchers:     []string{`{__name__=~"up.*"}`, `{__name__!~"down.*"}`},
			expectedMatchers:  []string{`{__name__=~"up.*"}`, `{__name__!~"down.*"}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		"__name__ not equal with value": {
			inputMatchers:     []string{`{__name__!="up"}`},
			expectedMatchers:  []string{`{__name__!="up"}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		"multiple matchers with mixed __name__ conditions": {
			inputMatchers:     []string{`{__name__!="", job="prometheus"}`, `{__name__="up"}`, `{__name__!=""}`, `{instance="localhost"}`},
			expectedMatchers:  []string{`{job="prometheus"}`, `{__name__="up"}`, `{instance="localhost"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		`multiple __name__!="" matchers in different sets`: {
			inputMatchers:     []string{`{__name__!="", job="prometheus"}`, `{__name__!="", instance="localhost"}`},
			expectedMatchers:  []string{`{job="prometheus"}`, `{instance="localhost"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		"complex matchers with special characters": {
			inputMatchers:     []string{`{__name__!="", job=~"prometheus.*", instance!="bad-host"}`},
			expectedMatchers:  []string{`{job=~"prometheus.*",instance!="bad-host"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		"quoted label names": {
			inputMatchers:     []string{`{__name__!="", "strange-label"="value"}`},
			expectedMatchers:  []string{`{"strange-label"="value"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		"label values with special characters": {
			inputMatchers:     []string{`{__name__!="", job="prometheus:9090", path="/metrics"}`},
			expectedMatchers:  []string{`{job="prometheus:9090",path="/metrics"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		"empty selector - no matchers inside braces": {
			inputMatchers:     []string{`{}`},
			expectedMatchers:  []string{`{}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		"invalid matcher syntax": {
			inputMatchers: []string{`{invalid syntax`},
			expectedError: true,
		},
		"malformed regex": {
			inputMatchers: []string{`{job=~"[invalid"}`},
			expectedError: true,
		},
		"mixed valid and optimizable matchers": {
			inputMatchers:     []string{`{job="test"}`, `{__name__!="", env="prod"}`, `{instance=~".*:9090"}`},
			expectedMatchers:  []string{`{job="test"}`, `{env="prod"}`, `{instance=~".*:9090"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		"unicode label values": {
			inputMatchers:     []string{`{__name__!="", job="测试", env="🚀"}`},
			expectedMatchers:  []string{`{job="测试",env="🚀"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		"large number of labels": {
			inputMatchers:     []string{`{__name__!="", label1="val1", label2="val2", label3="val3", label4="val4", label5="val5"}`},
			expectedMatchers:  []string{`{label1="val1",label2="val2",label3="val3",label4="val4",label5="val5"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		"matcher with escaped quotes": {
			inputMatchers:     []string{`{__name__!="", job="test\"with\"quotes"}`},
			expectedMatchers:  []string{`{job="test\"with\"quotes"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualMatchers, actualOptimized, err := optimizeLabelNamesRequestMatchers(testData.inputMatchers)

			if testData.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, testData.expectedOptimized, actualOptimized)
			assert.Equal(t, testData.expectedMatchers, actualMatchers)

			// Ensure the optimized matchers are still valid.
			_, err = parser.ParseMetricSelectors(actualMatchers)
			assert.NoError(t, err)
		})
	}
}
