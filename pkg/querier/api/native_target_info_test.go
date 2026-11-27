// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	//lint:ignore faillint Allow importing the propagation package, since it's an isolated package (doesn't come with many other dependencies).
	"github.com/grafana/mimir/pkg/util/propagation"
)

// The query-frontend's extractor puts a request's choice of how info() finds target_info into its context, and leaves
// the context alone for any other value, which the queriers reject, so that the query-frontend never fails a request
// over it.
func TestNativeTargetInfoModeExtractor_ExtractFromCarrier(t *testing.T) {
	for header, want := range map[string]struct{ native, set bool }{
		"native": {native: true, set: true},
		"join":   {native: false, set: true},
		"":       {},
		"both":   {},
	} {
		t.Run(header, func(t *testing.T) {
			headers := http.Header{}
			if header != "" {
				headers.Set(NativeTargetInfoHeader, header)
			}
			ctx, err := (&NativeTargetInfoModeExtractor{}).ExtractFromCarrier(t.Context(), propagation.HttpHeaderCarrier(headers))
			require.NoError(t, err)
			native, set := NativeTargetInfoFromContext(ctx)
			require.Equal(t, want, struct{ native, set bool }{native, set})
		})
	}
}
