// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/grafana/dskit/middleware"
	"github.com/stretchr/testify/assert"

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
		expectedErrType apierror.Type
	}{
		{
			url:             queryRangePathSuffix + "?query=up{&start=123&end=456&step=60s",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             queryRangePathSuffix + "?query=up{}&start=foo&end=456&step=60s",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             queryRangePathSuffix + "?query=up{}&start=123&end=bar&step=60s",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             queryRangePathSuffix + "?query=up{}&start=123&end=456&step=baz",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             queryRangePathSuffix + "?query=up{}&start=123&end=456&step=60s",
			expectedErrType: "",
		},
		{
			url:             instantQueryPathSuffix + "?query=up{",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             instantQueryPathSuffix + "?query=up{}&time=foo",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             instantQueryPathSuffix + "?query=up&start=123&end=456&step=60s",
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
		expectedErrType apierror.Type
	}{
		{
			url:             labelNamesPathSuffix + "?start=foo",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             labelNamesPathSuffix + "?end=foo",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             labelNamesPathSuffix + "?limit=foo",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             labelNamesPathSuffix + "",
			expectedErrType: "",
		},
		{
			url:             labelNamesPathSuffix + "?match=up{}&start=123&end=456&limit=100",
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

// TestCardinalityQueryRequestValidationRoundTripper only checks for expected API error types being returned,
// as the goal is to apply codec parsing early in the request cycle and return 400 errors for invalid requests.
// The exact error message for each different parse failure is tested extensively in the pkg/cardinality/reqeust tests.
func TestCardinalityQueryRequestValidationRoundTripper(t *testing.T) {
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

	rt := NewCardinalityQueryRequestValidationRoundTripper(http.DefaultTransport)
	for i, tc := range []struct {
		url             string
		expectedErrType apierror.Type
	}{
		{
			url:             cardinalityLabelNamesPathSuffix + "?selector=up{",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             cardinalityLabelNamesPathSuffix + "?limit=0&limit=10",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             cardinalityLabelNamesPathSuffix + "?count_method=foo",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             cardinalityLabelNamesPathSuffix,
			expectedErrType: "",
		},
		{
			url:             cardinalityLabelValuesPathSuffix + "?selector=up{",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             cardinalityLabelValuesPathSuffix + "?limit=0&limit=10",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             cardinalityLabelValuesPathSuffix + "?count_method=foo",
			expectedErrType: apierror.TypeBadData,
		},
		{
			// non-utf8 label name will be rejected even when we transition to UTF-8 label names
			url:             cardinalityLabelValuesPathSuffix + "?label_names[]=\\xbd\\xb2\\x3d\\xbc\\x20\\xe2\\x8c\\x98",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             cardinalityLabelValuesPathSuffix + "?label_names[]=foo",
			expectedErrType: "",
		},
		{
			url:             cardinalityActiveSeriesPathSuffix + "?selector=up{",
			expectedErrType: apierror.TypeBadData,
		},
		{
			url:             cardinalityActiveSeriesPathSuffix + "?selector=up{}",
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
