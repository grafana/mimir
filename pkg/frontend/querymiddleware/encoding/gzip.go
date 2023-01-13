// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
)

type GzipWrapperCodec struct {
	Inner Codec
}

func (c GzipWrapperCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, r); err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	if err := r.Close(); err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	return c.Inner.Decode(buf.Bytes())
}

func (c GzipWrapperCodec) Encode(resp querymiddleware.PrometheusResponse) ([]byte, error) {
	originalBytes, err := c.Inner.Encode(resp)
	if err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)

	if _, err := w.Write(originalBytes); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
