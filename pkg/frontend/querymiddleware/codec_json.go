// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"errors"

	jsoniter "github.com/json-iterator/go"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

const jsonMimeType = "application/json"

type jsonFormatter struct {
	encoder jsoniter.API
}

func newJSONFormatter(maxEncodedSize uint64) jsonFormatter {
	cfg := jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: true,
		MaxMarshalledBytes:     maxEncodedSize,
	}

	return jsonFormatter{encoder: cfg.Froze()}
}

func (j jsonFormatter) EncodeQueryResponse(resp *PrometheusResponse) ([]byte, error) {
	return j.marshal(resp)
}

func (j jsonFormatter) DecodeQueryResponse(buf []byte) (*PrometheusResponse, error) {
	var resp PrometheusResponse

	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (j jsonFormatter) EncodeLabelsResponse(resp *PrometheusLabelsResponse) ([]byte, error) {
	return j.marshal(resp)
}

func (j jsonFormatter) DecodeLabelsResponse(buf []byte) (*PrometheusLabelsResponse, error) {
	var resp PrometheusLabelsResponse

	if err := json.Unmarshal(buf, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (j jsonFormatter) EncodeSeriesResponse(resp *PrometheusSeriesResponse) ([]byte, error) {
	return j.marshal(resp)
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

func (j jsonFormatter) marshal(v interface{}) ([]byte, error) {
	b, err := j.encoder.Marshal(v)
	if err != nil {
		var limitErr jsoniter.ExceededMaxMarshalledBytesError

		if errors.As(err, &limitErr) {
			return nil, apierror.Newf(apierror.TypeTooLargeEntry, "JSON response is larger than the maximum allowed (%d bytes)", limitErr.MaxMarshalledBytes)
		}

		return nil, err
	}

	return b, nil

}
