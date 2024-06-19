// SPDX-License-Identifier: AGPL-3.0-only

package querytee

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func TestAddMissingTimeParam(t *testing.T) {
	// Mock the current time.
	originalTimeNow := timeNow
	timeNow = func() time.Time { return time.Date(2024, 6, 10, 20, 30, 40, 0, time.UTC) }
	t.Cleanup(func() { timeNow = originalTimeNow })

	testCases := map[string]struct {
		method    string
		urlParams url.Values
		form      url.Values

		expectedURLParams url.Values
		expectedForm      url.Values
	}{
		"GET with time in URL": {
			method: http.MethodGet,
			urlParams: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-19T01:23:45Z"},
			},

			expectedURLParams: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-19T01:23:45Z"},
			},
			expectedForm: url.Values{},
		},
		"GET without time in URL": {
			method: http.MethodGet,
			urlParams: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
			},

			expectedURLParams: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-10T20:30:40Z"},
			},
			expectedForm: url.Values{},
		},
		"POST with time in body": {
			method: http.MethodPost,
			form: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-19T01:23:45Z"},
			},

			expectedURLParams: url.Values{},
			expectedForm: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-19T01:23:45Z"},
			},
		},
		"POST without time in body": {
			method: http.MethodPost,
			form: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
			},

			expectedURLParams: url.Values{},
			expectedForm: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-10T20:30:40Z"},
			},
		},
		"POST with time in URL": {
			method: http.MethodPost,
			urlParams: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-19T01:23:45Z"},
			},

			expectedURLParams: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-19T01:23:45Z"},
			},
			expectedForm: url.Values{},
		},
		"POST without time in URL": {
			method: http.MethodPost,
			urlParams: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
			},

			expectedURLParams: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-10T20:30:40Z"},
			},
			expectedForm: url.Values{},
		},
	}

	logger, _ := spanlogger.New(context.Background(), "test")

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			var req *http.Request
			var err error
			var body []byte

			if testCase.method == http.MethodGet {
				require.Nil(t, testCase.form, "invalid test case: GET request should not have body")
				req, err = http.NewRequest(testCase.method, "/blah?"+testCase.urlParams.Encode(), nil)
				require.NoError(t, err)
				body = nil
			} else {
				encoded := testCase.form.Encode()
				reader := strings.NewReader(encoded)
				req, err = http.NewRequest(testCase.method, "/blah?"+testCase.urlParams.Encode(), reader)
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.Header.Set("Content-Length", strconv.Itoa(reader.Len()))

				// ProxyEndpoint.executeBackendRequests parses the form body, so we do the same, for consistency.
				require.NoError(t, req.ParseForm())

				body = []byte(encoded)
			}

			transformed, transformedBody, err := AddMissingTimeParam(req, body, logger)
			require.NoError(t, err)
			require.Equal(t, testCase.method, transformed.Method)
			require.Equal(t, "/blah", transformed.URL.Path)
			require.Equal(t, testCase.expectedURLParams, transformed.URL.Query())

			if len(testCase.expectedForm) == 0 {
				require.Empty(t, transformedBody)
			} else {
				parsedBody, err := url.ParseQuery(string(transformedBody))
				require.NoError(t, err)
				require.Equal(t, testCase.expectedForm, parsedBody)

				// Make sure we've updated both places, for consistency.
				require.Equal(t, testCase.expectedForm, transformed.Form)

				// Make sure we've updated the Content-Length header to match the new request body length.
				require.Equal(t, int64(len(transformedBody)), transformed.ContentLength)
				require.Equal(t, strconv.Itoa(len(transformedBody)), transformed.Header.Get("Content-Length"))
			}
		})
	}
}
