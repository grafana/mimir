// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/dskit/middleware"
	apierror "github.com/grafana/mimir/pkg/api/error"
)

// TestMetricsQueryRequestValidationRoundTripper only checks for expected API error types being returned,
// as the goal is to apply codec parsing early in the request cycle and return 400 errors for invalid requests.
// The exact error message for each different parse failure is tested extensively in the codec tests.
func TestMetricsQueryRequestValidationRoundTripper(t *testing.T) {
	srv := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				var err error
				_, err = w.Write(nil)

				if err != nil {
					t.Fatal(err)
				}
			}),
		),
	)
	defer srv.Close()

	rt := NewMetricsQueryRequestValidationRoundTripper(newTestPrometheusCodec(), http.DefaultTransport)

	for i, tc := range []struct {
		url             string
		expected        MetricsQueryRequest
		expectedErrType apierror.Type
	}{
		{
			url:             "/api/v1/query_range?query=up{&start=123&end=456&step=60s",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             "/api/v1/query_range?query=up{}&start=foo&end=456&step=60s",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             "/api/v1/query_range?query=up&start=123&end=bar&step=60s",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             "/api/v1/query_range?query=up&start=123&end=456&step=baz",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             "/api/v1/query_range?query=up&start=123&end=456&step=60s",
			expectedErrType: "",
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, srv.URL+tc.url, nil)
			if err != nil {
				t.Fatal(err)
			}

			_, err = rt.RoundTrip(req)
			if tc.expectedErrType == "" {
				assert.NoError(t, err)
				return
			}

			apiErr := &apierror.APIError{}

			assert.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tc.expectedErrType, apiErr.Type)

		})
	}
}

// TestLabelsQueryRequestValidationRoundTripper only checks for expected API error types being returned,
// as the goal is to apply codec parsing early in the request cycle and return 400 errors for invalid requests.
// The exact error message for each different parse failure is tested extensively in the codec tests.
func TestLabelsQueryRequestValidationRoundTripper(t *testing.T) {
	srv := httptest.NewServer(
		middleware.AuthenticateUser.Wrap(
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				var err error
				_, err = w.Write(nil)

				if err != nil {
					t.Fatal(err)
				}
			}),
		),
	)
	defer srv.Close()

	rt := NewLabelsQueryRequestValidationRoundTripper(newTestPrometheusCodec(), http.DefaultTransport)

	for i, tc := range []struct {
		url             string
		expected        MetricsQueryRequest
		expectedErrType apierror.Type
	}{
		{
			url:             "/api/v1/labels?start=foo",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             "/api/v1/labels?end=foo",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             "/api/v1/labels?limit=foo",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             "/api/v1/labels",
			expectedErrType: "",
		},
		{
			url:             "/api/v1/labels?match=up{}&start=123&end=456&limit=100",
			expectedErrType: "",
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, srv.URL+tc.url, nil)
			if err != nil {
				t.Fatal(err)
			}

			_, err = rt.RoundTrip(req)
			if tc.expectedErrType == "" {
				assert.NoError(t, err)
				return
			}

			apiErr := &apierror.APIError{}

			assert.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tc.expectedErrType, apiErr.Type)

		})
	}
}
