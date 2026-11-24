// SPDX-License-Identifier: AGPL-3.0-only

package requestoptions

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/propagation"
)

func TestStartTimestampsExtractor(t *testing.T) {
	require.False(t, UseStartTimestampsFromContext(t.Context()))

	for _, value := range []string{"", "false", "true", "TRUE", "1", "0", "invalid"} {
		t.Run(value, func(t *testing.T) {
			headers := http.Header{}
			headers.Set(UseStartTimestampsHeader, value)
			ctx, err := (&StartTimestampsExtractor{}).ExtractFromCarrier(t.Context(), propagation.HttpHeaderCarrier(headers))
			if value == "invalid" {
				require.EqualError(t, err, `invalid value "invalid" for X-Prometheus-Use-Start-Timestamps header: must be a boolean`)
				require.Equal(t, apierror.TypeBadData, apierror.TypeForError(err, apierror.TypeNone))
				return
			}

			require.NoError(t, err)
			require.Equal(t, value == "true" || value == "TRUE" || value == "1", UseStartTimestampsFromContext(ctx))

			parsed, err := ParseUseStartTimestamps(value)
			require.NoError(t, err)
			if value == "" {
				require.Nil(t, parsed)
			} else {
				require.NotNil(t, parsed)
				require.Equal(t, UseStartTimestampsFromContext(ctx), *parsed)
			}
		})
	}
}
