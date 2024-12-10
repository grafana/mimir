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
		body      url.Values

		expectedURLParams url.Values
		expectedBody      url.Values
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
			expectedBody: url.Values{},
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
			expectedBody: url.Values{},
		},
		"POST with time in body": {
			method: http.MethodPost,
			body: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-19T01:23:45Z"},
			},

			expectedURLParams: url.Values{},
			expectedBody: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-19T01:23:45Z"},
			},
		},
		"POST without time in body": {
			method: http.MethodPost,
			body: url.Values{
				"query":   []string{"sum(abc)"},
				"timeout": []string{"60s"},
			},

			expectedURLParams: url.Values{},
			expectedBody: url.Values{
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
			expectedBody: url.Values{},
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
			expectedBody: url.Values{},
		},
		"POST with parameters in both URL and body, and no time in either place": {
			method: http.MethodPost,
			urlParams: url.Values{
				"query": []string{"sum(abc)"},
			},
			body: url.Values{
				"timeout": []string{"60s"},
			},

			expectedURLParams: url.Values{
				"query": []string{"sum(abc)"},
			},
			expectedBody: url.Values{
				"timeout": []string{"60s"},
				"time":    []string{"2024-06-10T20:30:40Z"},
			},
		},
		"POST with parameters in both URL and body, and time in URL": {
			method: http.MethodPost,
			urlParams: url.Values{
				"query": []string{"sum(abc)"},
				"time":  []string{"2024-06-19T01:23:45Z"},
			},
			body: url.Values{
				"timeout": []string{"60s"},
			},

			expectedURLParams: url.Values{
				"query": []string{"sum(abc)"},
				"time":  []string{"2024-06-19T01:23:45Z"},
			},
			expectedBody: url.Values{
				"timeout": []string{"60s"},
			},
		},
		"POST with parameters in both URL and body, and time in body": {
			method: http.MethodPost,
			urlParams: url.Values{
				"query": []string{"sum(abc)"},
			},
			body: url.Values{
				"time":    []string{"2024-06-19T01:23:45Z"},
				"timeout": []string{"60s"},
			},

			expectedURLParams: url.Values{
				"query": []string{"sum(abc)"},
			},
			expectedBody: url.Values{
				"time":    []string{"2024-06-19T01:23:45Z"},
				"timeout": []string{"60s"},
			},
		},
	}

	logger, _ := spanlogger.New(context.Background(), "test")

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			var req *http.Request
			var err error
			var body []byte

			if testCase.method == http.MethodGet {
				require.Nil(t, testCase.body, "invalid test case: GET request should not have body")
				req, err = http.NewRequest(testCase.method, "/blah?"+testCase.urlParams.Encode(), nil)
				require.NoError(t, err)
				body = nil
			} else {
				encoded := testCase.body.Encode()
				reader := strings.NewReader(encoded)
				req, err = http.NewRequest(testCase.method, "/blah?"+testCase.urlParams.Encode(), reader)
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.Header.Set("Content-Length", strconv.Itoa(reader.Len()))
				body = []byte(encoded)
			}

			transformed, transformedBody, err := AddMissingTimeParam(req, body, logger)
			require.NoError(t, err)
			require.Equal(t, testCase.method, transformed.Method)
			require.Equal(t, "/blah", transformed.URL.Path)
			require.Equal(t, testCase.expectedURLParams, transformed.URL.Query())

			if len(testCase.expectedBody) == 0 {
				require.Empty(t, transformedBody)
				require.Empty(t, transformed.PostForm)
			} else {
				parsedBody, err := url.ParseQuery(string(transformedBody))
				require.NoError(t, err)
				require.Equal(t, testCase.expectedBody, parsedBody)

				// Make sure we've updated both the body and PostForm, for consistency.
				require.Equal(t, testCase.expectedBody, transformed.PostForm)

				// Make sure we've updated the Content-Length header to match the new request body length.
				require.Equal(t, int64(len(transformedBody)), transformed.ContentLength)
				require.Equal(t, strconv.Itoa(len(transformedBody)), transformed.Header.Get("Content-Length"))
			}

			expectedForm := testCase.expectedURLParams

			for k, v := range testCase.expectedBody {
				expectedForm[k] = v
			}

			require.Equal(t, expectedForm, transformed.Form)
		})
	}
}
