// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"github.com/golang/snappy"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
)

type SnappyWrapperCodec struct {
	Inner Codec
}

func (c SnappyWrapperCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	originalBytes, err := snappy.Decode(nil, b)
	if err != nil {
		return querymiddleware.PrometheusResponse{}, nil
	}

	return c.Inner.Decode(originalBytes)
}

func (c SnappyWrapperCodec) Encode(resp querymiddleware.PrometheusResponse) ([]byte, error) {
	originalBytes, err := c.Inner.Encode(resp)
	if err != nil {
		return nil, err
	}

	return snappy.Encode(nil, originalBytes), nil
}
