// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	mimirtest "github.com/grafana/mimir/pkg/util/test"
)

func TestLabelsQueryOptimizer_RoundTrip(t *testing.T) {
	const userID = "user-1"

	tests := map[string]struct {
		optimizerEnabled         bool
		reqPath                  string
		reqData                  url.Values
		expectedOptimized        bool
		expectedDownstreamParams url.Values
	}{
		//
		// Label names API
		//

		`should optimize labels request with __name__!="" matcher`: {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="prometheus"}`}},
			expectedOptimized:        true,
		},
		"should optimize labels request with '.*' regex matcher": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{job=~".*", env="prod"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{env="prod"}`}},
			expectedOptimized:        true,
		},
		"should optimize multiple matchers": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`, `{__name__!="", instance="localhost"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="prometheus"}`, `{instance="localhost"}`}},
			expectedOptimized:        true,
		},
		"should handle mixed optimizable and non-optimizable matchers": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{job="test"}`, `{__name__!="", env="prod"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="test"}`, `{env="prod"}`}},
			expectedOptimized:        true,
		},
		"should handle matchers with quoted label names": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", "strange-label"="value"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{"strange-label"="value"}`}},
			expectedOptimized:        true,
		},
		"should handle matchers with unicode label values": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="测试", env="🚀"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="测试",env="🚀"}`}},
			expectedOptimized:        true,
		},
		"should pass through when optimization disabled": {
			optimizerEnabled:         false,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
			expectedOptimized:        false,
		},
		"should pass through when no optimization needed": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="prometheus"}`}},
			expectedOptimized:        false,
		},
		"should pass through when one of the matchers set is an empty matcher": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{`{}`, `{__name__!="", job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{}`, `{__name__!="", job="prometheus"}`}},
			expectedOptimized:        false,
		},
		"should pass through when no matchers": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{},
			expectedDownstreamParams: url.Values{},
			expectedOptimized:        false,
		},
		"should pass through when malformed matchers": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/labels",
			reqData:                  url.Values{"match[]": []string{"{invalid syntax"}},
			expectedDownstreamParams: url.Values{"match[]": []string{"{invalid syntax"}},
			expectedOptimized:        false,
		},

		//
		// Label values API
		//

		`should optimize label values request with __name__!="" matcher`: {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="prometheus"}`}},
			expectedOptimized:        true,
		},
		"should optimize label values request with '.*' regex matcher": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{"match[]": []string{`{job=~".*", env="prod"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{env="prod"}`}},
			expectedOptimized:        true,
		},
		"should optimize multiple matchers for label values": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`, `{__name__!="", instance="localhost"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="prometheus"}`, `{instance="localhost"}`}},
			expectedOptimized:        true,
		},
		"should handle mixed optimizable and non-optimizable matchers for label values": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{"match[]": []string{`{job="test"}`, `{__name__!="", env="prod"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="test"}`, `{env="prod"}`}},
			expectedOptimized:        true,
		},
		"should handle matchers with quoted label names for label values": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", "strange-label"="value"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{"strange-label"="value"}`}},
			expectedOptimized:        true,
		},
		"should handle matchers with unicode label values for label values": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="测试", env="🚀"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="测试",env="🚀"}`}},
			expectedOptimized:        true,
		},
		"should pass through label values when optimization disabled": {
			optimizerEnabled:         false,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
			expectedOptimized:        false,
		},
		"should pass through label values when no optimization needed": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{"match[]": []string{`{job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{job="prometheus"}`}},
			expectedOptimized:        false,
		},
		"should pass through label values when one of the matchers set is an empty matcher": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{"match[]": []string{`{}`, `{__name__!="", job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{}`, `{__name__!="", job="prometheus"}`}},
			expectedOptimized:        false,
		},
		"should pass through label values when no matchers": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{},
			expectedDownstreamParams: url.Values{},
			expectedOptimized:        false,
		},
		"should pass through label values when malformed matchers": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/label/job/values",
			reqData:                  url.Values{"match[]": []string{"{invalid syntax"}},
			expectedDownstreamParams: url.Values{"match[]": []string{"{invalid syntax"}},
			expectedOptimized:        false,
		},

		//
		// Unsupported APIs
		//

		"should pass through on a non supported API": {
			optimizerEnabled:         true,
			reqPath:                  "/api/v1/series",
			reqData:                  url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
			expectedDownstreamParams: url.Values{"match[]": []string{`{__name__!="", job="prometheus"}`}},
			expectedOptimized:        false,
		},
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
						downstreamHeaders   http.Header
						err                 error
					)

					// Mock the downstream and capture the request.
					downstream := RoundTripFunc(func(req *http.Request) (*http.Response, error) {
						downstreamCalled = true

						// Parse the request form to capture parameters.
						require.NoError(t, req.ParseForm())
						downstreamReqParams = req.Form

						// Capture the headers too.
						downstreamHeaders = req.Header.Clone()

						return &http.Response{StatusCode: http.StatusOK}, nil
					})

					// Inject other parameters to the request and expected values, to ensure the request is serialized
					// and deserialized correctly.
					testData.reqData["start"] = []string{time.Now().Format(time.RFC3339)}
					testData.reqData["end"] = []string{time.Now().Format(time.RFC3339)}
					testData.reqData["limit"] = []string{"10"}

					if testData.expectedOptimized {
						// When we re-encode the start and end parameters we always use the epoch timestamp.
						// It's fine, because it's idempotent.
						testData.expectedDownstreamParams["start"] = []string{encodeTime(mustParseTime(testData.reqData["start"][0]))}
						testData.expectedDownstreamParams["end"] = []string{encodeTime(mustParseTime(testData.reqData["end"][0]))}
					} else {
						testData.expectedDownstreamParams["start"] = testData.reqData["start"]
						testData.expectedDownstreamParams["end"] = testData.reqData["end"]
					}
					testData.expectedDownstreamParams["limit"] = testData.reqData["limit"]

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
					reqCtx := user.InjectOrgID(context.Background(), userID)
					req = req.WithContext(reqCtx)
					require.NoError(t, user.InjectOrgIDIntoHTTPRequest(reqCtx, req))

					// Create the labels query optimizer
					reg := prometheus.NewPedanticRegistry()
					codec := newTestCodec()
					optimizer := newLabelsQueryOptimizer(codec, limits, downstream, mimirtest.NewTestingLogger(t), reg)

					// Execute the request
					_, err = optimizer.RoundTrip(req)
					require.NoError(t, err)

					// Ensure the downstream has been called with the expected params and headers.
					require.True(t, downstreamCalled)
					require.Equal(t, testData.expectedDownstreamParams, downstreamReqParams)
					require.Equal(t, userID, downstreamHeaders.Get(user.OrgIDHeaderName))

					// Assert on metrics.
					expectedTotal := 0
					expectedRewritten := 0
					if testData.optimizerEnabled && (testData.reqPath == "/api/v1/labels" || strings.HasPrefix(testData.reqPath, "/api/v1/label/")) {
						expectedTotal = 1
					}
					if testData.expectedOptimized {
						expectedRewritten = 1
					}

					assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
						# HELP cortex_query_frontend_labels_optimizer_queries_total Total number of label API requests processed by the optimizer when enabled.
						# TYPE cortex_query_frontend_labels_optimizer_queries_total counter
						cortex_query_frontend_labels_optimizer_queries_total %d

						# HELP cortex_query_frontend_labels_optimizer_queries_rewritten_total Total number of label API requests that have been optimized.
						# TYPE cortex_query_frontend_labels_optimizer_queries_rewritten_total counter
						cortex_query_frontend_labels_optimizer_queries_rewritten_total %d

						# HELP cortex_query_frontend_labels_optimizer_queries_failed_total Total number of label API requests that failed to get optimized.
						# TYPE cortex_query_frontend_labels_optimizer_queries_failed_total counter
						cortex_query_frontend_labels_optimizer_queries_failed_total 0
					`, expectedTotal, expectedRewritten))))
				})
			}
		})
	}
}

func TestOptimizeLabelsRequestMatchers(t *testing.T) {
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
		`single __name__!="" matcher with 1 other matcher`: {
			inputMatchers:     []string{`{__name__!="", job="prometheus"}`},
			expectedMatchers:  []string{`{job="prometheus"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		`single __name__!="" matcher with multiple other matchers`: {
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
		"__name__ regex matcher": {
			inputMatchers:     []string{`{__name__!~"down.*"}`},
			expectedMatchers:  []string{`{__name__!~"down.*"}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		"__name__ not equal matcher": {
			inputMatchers:     []string{`{__name__!="up"}`},
			expectedMatchers:  []string{`{__name__!="up"}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		`multiple matcher sets with mixed __name__ conditions, including the __name__!="" matcher set`: {
			// Different matcher sets are in "or" condition. Since there's a matcher set with just `{__name__!=""}`
			// that match all series, we can remove all matchers.
			inputMatchers:     []string{`{__name__!="", job="prometheus"}`, `{__name__="up"}`, `{__name__!=""}`, `{instance="localhost"}`},
			expectedMatchers:  []string{},
			expectedOptimized: true,
			expectedError:     false,
		},
		`multiple matcher sets with __name__!="" matcher but not standalone`: {
			inputMatchers:     []string{`{__name__!="", job="prometheus"}`, `{__name__!="", instance="localhost"}`},
			expectedMatchers:  []string{`{job="prometheus"}`, `{instance="localhost"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		"empty matcher": {
			// The empty matcher must be preserved because it's an invalid matcher, and we
			// want the downstream to generate the proper validation error.
			inputMatchers:     []string{`{}`},
			expectedMatchers:  []string{`{}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		"empty matcher in a list of non-optimized matchers": {
			// The empty matcher must be preserved because it's an invalid matcher, and we
			// want the downstream to generate the proper validation error.
			inputMatchers:     []string{`{}`, `{job="prometheus:9090",path="/metrics"}`},
			expectedMatchers:  []string{`{}`, `{job="prometheus:9090",path="/metrics"}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		"empty matcher in a list of optimised matchers": {
			// The empty matcher must be preserved because it's an invalid matcher, and we
			// want the downstream to generate the proper validation error.
			inputMatchers:     []string{`{}`, `{__name__!="", job="prometheus:9090"}`},
			expectedMatchers:  []string{`{}`, `{__name__!="", job="prometheus:9090"}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		"quoted label names": {
			inputMatchers:     []string{`{__name__!="", "strange-label"="value"}`},
			expectedMatchers:  []string{`{"strange-label"="value"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		"unicode label values": {
			inputMatchers:     []string{`{__name__!="", job="测试", env="🚀"}`},
			expectedMatchers:  []string{`{job="测试",env="🚀"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		"matcher with escaped quotes": {
			inputMatchers:     []string{`{__name__!="", job="test\"with\"quotes"}`},
			expectedMatchers:  []string{`{job="test\"with\"quotes"}`},
			expectedOptimized: true,
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
		"regex matcher with '.*' value": {
			// The .* matcher must be preserved because it's an invalid matcher when it's the only one in a set,
			// and we want the downstream to generate the proper validation error.
			inputMatchers:     []string{`{job=~".*"}`},
			expectedMatchers:  []string{`{job=~".*"}`},
			expectedOptimized: false,
			expectedError:     false,
		},
		"regex matcher with '.*' value combined with other matchers": {
			inputMatchers:     []string{`{job=~".*", env="prod"}`},
			expectedMatchers:  []string{`{env="prod"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
		`regex matcher with '.*' value and __name__!="" matcher`: {
			inputMatchers:     []string{`{__name__!="", job=~".*", env="prod"}`},
			expectedMatchers:  []string{`{env="prod"}`},
			expectedOptimized: true,
			expectedError:     false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualMatchers, actualOptimized, err := optimizeLabelsRequestMatchers(testData.inputMatchers)

			if testData.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, testData.expectedOptimized, actualOptimized)
			assert.Equal(t, testData.expectedMatchers, actualMatchers)

			// Ensure the optimized matchers are still valid.
			_, err = promqlext.NewPromQLParser().ParseMetricSelectors(actualMatchers)
			assert.NoError(t, err)
		})
	}
}
