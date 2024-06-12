// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestParseRemoteReadRequestWithoutConsumingBody(t *testing.T) {
	testCases := map[string]struct {
		reqBody               func() io.ReadCloser
		contentLength         int
		expectedErrorContains string
		expectedErrorIs       error
		expectedParams        url.Values
	}{
		"no body": {
			reqBody: func() io.ReadCloser {
				return nil
			},
			expectedParams: make(url.Values),
		},
		"valid body": {
			reqBody: func() io.ReadCloser {
				remoteReadRequest := &prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "some_metric"},
								{Name: "foo", Type: prompb.LabelMatcher_RE, Value: ".*bar.*"},
							},
							StartTimestampMs: 0,
							EndTimestampMs:   42,
						},
						{
							Matchers: []*prompb.LabelMatcher{
								{Name: "__name__", Type: prompb.LabelMatcher_EQ, Value: "up"},
							},
							StartTimestampMs: 10,
							EndTimestampMs:   20,
							Hints: &prompb.ReadHints{
								StepMs: 1000,
							},
						},
					},
				}
				data, _ := proto.Marshal(remoteReadRequest) // Ignore error, if this fails, the test will fail.
				compressed := snappy.Encode(nil, data)
				return io.NopCloser(bytes.NewReader(compressed))
			},
			expectedParams: url.Values{
				"start_0":    []string{"0"},
				"end_0":      []string{"42"},
				"matchers_0": []string{"__name__=\"some_metric\",foo=~\".*bar.*\""},
				"start_1":    []string{"10"},
				"end_1":      []string{"20"},
				"matchers_1": []string{"__name__=\"up\""},
				"hints_1":    []string{"{\"step_ms\":1000}"},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			req := &http.Request{
				Body: tc.reqBody(),
			}
			params, err := ParseRemoteReadRequestWithoutConsumingBody(req)
			if err != nil {
				if tc.expectedErrorIs != nil {
					require.ErrorIs(t, err, tc.expectedErrorIs)
					require.Contains(t, err.Error(), tc.expectedErrorContains)
				} else {
					require.NoError(t, err)
				}
			}
			require.Equal(t, tc.expectedParams, params)

			// Check that we can still read the Body after parsing.
			if req.Body != nil {
				bodyBytes, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.NoError(t, req.Body.Close())
				require.NotEmpty(t, bodyBytes)
			}
		})
	}
}
