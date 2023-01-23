// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
)

type OriginalQueryMiddlewareProtobufCodec struct{}

func (c OriginalQueryMiddlewareProtobufCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	var resp querymiddleware.PrometheusResponse

	if err := resp.Unmarshal(b); err != nil {
		return querymiddleware.PrometheusResponse{}, nil
	}

	return resp, nil
}

func (c OriginalQueryMiddlewareProtobufCodec) Encode(resp querymiddleware.PrometheusResponse) ([]byte, error) {
	return resp.Marshal()
}
