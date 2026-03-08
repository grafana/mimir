// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/propagation"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_labelAccessMiddleware_Wrap(t *testing.T) {
	tests := map[string]struct {
		url           string
		expectedError string
		useLBAC       bool
	}{
		"expected error if request with LBAC and url ends with /cardinality/label_values": {
			url:           "/cardinality/label_values",
			expectedError: "/cardinality/label_values endpoint doesn't support access policies with label-based access control",
			useLBAC:       true,
		},
		"expected error if request with LBAC and url ends with /cardinality/label_names": {
			url:           "/cardinality/label_names",
			expectedError: "/cardinality/label_names endpoint doesn't support access policies with label-based access control",
			useLBAC:       true,
		},
		"expected error if request with LBAC and url ends with /cardinality/label_names ignoring request params": {
			url:           "/cardinality/label_names?limit=500",
			expectedError: "/cardinality/label_names endpoint doesn't support access policies with label-based access control",
			useLBAC:       true,
		},
		"expect error on active_series request with LBAC enabled": {
			url:           "/cardinality/active_series",
			expectedError: "/cardinality/active_series endpoint doesn't support access policies with label-based access control",
			useLBAC:       true,
		},
		"expect error on active_series request with parameters and LBAC enabled": {
			url:           "/cardinality/active_series?selector=foo",
			expectedError: "/cardinality/active_series endpoint doesn't support access policies with label-based access control",
			useLBAC:       true,
		},
		"expected no errors if request with LBAC but url is not related to cardinality analysis": {
			url: "/any-url",
		},
		"expected no errors if request without LBAC and url ends with /cardinality/label_values": {
			url: "/cardinality/label_values",
		},
		"expected no errors if request without LBAC and url ends with /cardinality/label_names": {
			url: "/cardinality/label_names",
		},
		"expect no errors for active_series request without LBAC": {
			url: "/cardinality/active_series",
		},
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			l := &labelAccessMiddleware{
				logger: util_log.Logger,
			}
			handler := l.Wrap(mockHandler{})
			request, err := http.NewRequestWithContext(context.Background(), "GET", data.url, http.NoBody)
			require.NoError(t, err)
			if data.useLBAC {
				headerValue := "user-2:%7Bfoo=%22car%22%7D"
				request.Header.Add(HTTPHeaderKey, headerValue)
			}
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			body := recorder.Result().Body
			bodyContent, err := io.ReadAll(body)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = body.Close()
			})
			if len(data.expectedError) > 0 {
				require.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
				require.Contains(t, string(bodyContent), data.expectedError)
				return
			}
			require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
			require.Equal(t, "mock ok response", string(bodyContent))
		})
	}
}

type mockHandler struct {
}

func (m mockHandler) ServeHTTP(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusOK)
	fmt.Fprint(writer, "mock ok response")
}

func TestLabelAccessExtractor(t *testing.T) {
	testCases := map[string]struct {
		metadata          map[string][]string
		expectedPolicySet LabelPolicySet
		expectError       bool
	}{
		"no matching metadata value": {
			metadata:          map[string][]string{},
			expectedPolicySet: LabelPolicySet{},
			expectError:       true, // errNoMatcherSource expected
		},
		"single policy": {
			metadata: map[string][]string{HTTPHeaderKey: {"user-1:%7Bfoo=%22bar-1%22%7D"}},
			expectedPolicySet: LabelPolicySet{
				"user-1": []*LabelPolicy{
					{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar-1")}},
				},
			},
		},
		"multiple policies": {
			metadata: map[string][]string{HTTPHeaderKey: {"user-1:%7Bfoo=%22bar-1%22%7D", "user-2:%7Bfoo=%22bar-2%22%7D"}},
			expectedPolicySet: LabelPolicySet{
				"user-1": []*LabelPolicy{
					{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar-1")}},
				},
				"user-2": []*LabelPolicy{
					{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar-2")}},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			extractor := NewExtractor()
			ctx, err := extractor.ExtractFromCarrier(context.Background(), propagation.MapCarrier(testCase.metadata))
			require.NoError(t, err)

			policySet, err := ExtractLabelMatchersContext(ctx)
			if testCase.expectError {
				require.Error(t, err)
				assert.Equal(t, errNoMatcherSource, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedPolicySet, policySet)
			}
		})
	}
}
