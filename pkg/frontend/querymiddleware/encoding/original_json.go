// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	jsoniter "github.com/json-iterator/go"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
)

type OriginalJsonCodec struct{}

var (
	json = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: true,
	}.Froze()
)

func (c OriginalJsonCodec) Decode(b []byte) (querymiddleware.PrometheusResponse, error) {
	var resp querymiddleware.PrometheusResponse

	if err := json.Unmarshal(b, &resp); err != nil {
		return querymiddleware.PrometheusResponse{}, err
	}

	return resp, nil
}

func (c OriginalJsonCodec) Encode(resp querymiddleware.PrometheusResponse) ([]byte, error) {
	return json.Marshal(resp)
}
