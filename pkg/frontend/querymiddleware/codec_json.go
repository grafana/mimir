// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

const jsonMimeType = "application/json"

type jsonFormat struct{}

func (j jsonFormat) EncodeResponse(resp *PrometheusResponse) ([]byte, error) {
	return json.Marshal(resp)
}

func (j jsonFormat) DecodeResponse(buf []byte) (*PrometheusResponse, error) {
	var resp PrometheusResponse

	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (j jsonFormat) Name() string {
	return formatJSON
}
