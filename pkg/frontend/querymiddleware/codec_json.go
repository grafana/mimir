// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

const jsonMimeType = "application/json"

type jsonFormatter struct{}

func (j jsonFormatter) EncodeQueryResponse(resp *PrometheusResponse) ([]byte, error) {
	return json.Marshal(resp)
}

func (j jsonFormatter) DecodeQueryResponse(buf []byte) (*PrometheusResponse, error) {
	var resp PrometheusResponse

	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (j jsonFormatter) EncodeLabelsResponse(resp *PrometheusLabelsResponse) ([]byte, error) {
	return json.Marshal(resp)
}

func (j jsonFormatter) DecodeLabelsResponse(buf []byte) (*PrometheusLabelsResponse, error) {
	var resp PrometheusLabelsResponse

	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (j jsonFormatter) EncodeSeriesResponse(resp *PrometheusSeriesResponse) ([]byte, error) {
	return json.Marshal(resp)
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
