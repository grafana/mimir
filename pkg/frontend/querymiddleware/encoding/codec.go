// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import "github.com/grafana/mimir/pkg/frontend/querymiddleware"

type Codec interface {
	// TODO: in the real implementation, rather than consuming / emitting byte slices, we'd probably want to use io.Reader and io.Writer.

	Encode(r querymiddleware.PrometheusResponse) ([]byte, error)
	Decode(b []byte) (querymiddleware.PrometheusResponse, error)
}
