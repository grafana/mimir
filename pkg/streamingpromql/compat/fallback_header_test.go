// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/propagation"
)

func TestEngineFallbackExtractor(t *testing.T) {
	testCases := map[string]struct {
		headers http.Header

		expectFallback bool
		expectedError  string
	}{
		"no headers": {
			headers:        http.Header{},
			expectFallback: false,
		},
		"unrelated header": {
			headers: http.Header{
				"Content-Type": []string{"application/blah"},
			},
			expectFallback: false,
		},
		"force fallback header is present, but does not have expected value": {
			headers: http.Header{
				"X-Mimir-Force-Prometheus-Engine": []string{"blah"},
			},
			expectedError: "invalid value 'blah' for 'X-Mimir-Force-Prometheus-Engine' header, must be exactly 'true' or not set",
		},
		"force fallback header is present, and does have expected value": {
			headers: http.Header{
				"X-Mimir-Force-Prometheus-Engine": []string{"true"},
			},
			expectFallback: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			extractor := &EngineFallbackExtractor{}
			ctx, err := extractor.ExtractFromCarrier(context.Background(), propagation.HttpHeaderCarrier(testCase.headers))

			if testCase.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, testCase.expectFallback, isForceFallbackEnabled(ctx))
			} else {
				require.EqualError(t, err, testCase.expectedError)
			}
		})
	}
}
