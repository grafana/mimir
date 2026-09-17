// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"io"

	v1 "github.com/prometheus/prometheus/web/api/v1"
)

const jsonMimeType = "application/json"

type jsonFormatter struct{}

func (j jsonFormatter) EncodeQueryResponseTo(w io.Writer, resp *PrometheusResponse) error {
	return jsonStreamEncode(w, resp)
}

func (j jsonFormatter) DecodeQueryResponse(buf []byte) (*PrometheusResponse, error) {
	var resp PrometheusResponse

	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (j jsonFormatter) EncodeLabelsResponseTo(w io.Writer, resp *PrometheusLabelsResponse) error {
	return jsonStreamEncode(w, resp)
}

func (j jsonFormatter) DecodeLabelsResponse(buf []byte) (*PrometheusLabelsResponse, error) {
	var resp PrometheusLabelsResponse

	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (j jsonFormatter) EncodeSeriesResponseTo(w io.Writer, resp *PrometheusSeriesResponse) error {
	return jsonStreamEncode(w, resp)
}

func (j jsonFormatter) DecodeSeriesResponse(buf []byte) (*PrometheusSeriesResponse, error) {
	var resp PrometheusSeriesResponse

	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (j jsonFormatter) Name() string {
	return formatJSON
}

func (j jsonFormatter) ContentType() v1.MIMEType {
	return v1.MIMEType{Type: "application", SubType: "json"}
}

// jsonStreamEncode encodes v into w using a pooled jsoniter stream. This avoids
// the repeated buffer-doubling allocations that occur when json.NewEncoder starts
// with a small buffer and must grow to fit the full response.
func jsonStreamEncode(w io.Writer, v interface{}) error {
	stream := json.BorrowStream(w)
	defer json.ReturnStream(stream)
	stream.WriteVal(v)

	// Append a trailing newline to match the output of json.Encoder.Encode, which
	// tests and callers rely on for byte-exact comparisons.
	stream.WriteRaw("\n")
	if stream.Error != nil {
		return stream.Error
	}
	err := stream.Flush()
	return err
}
