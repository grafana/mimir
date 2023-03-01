// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

type decoder interface {
	ContentType() string
	Decode(body []byte) (promql.Vector, error)
}

type jsonDecoder struct{}

func (jsonDecoder) ContentType() string {
	return "application/json"
}

func (d jsonDecoder) Decode(body []byte) (promql.Vector, error) {
	var apiResp struct {
		Status    string          `json:"status"`
		Data      json.RawMessage `json:"data"`
		ErrorType string          `json:"errorType"`
		Error     string          `json:"error"`
	}
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&apiResp); err != nil {
		return promql.Vector{}, err
	}
	if apiResp.Status == statusError {
		return promql.Vector{}, fmt.Errorf("query response error: %s", apiResp.Error)
	}
	v := struct {
		Type   model.ValueType `json:"resultType"`
		Result json.RawMessage `json:"result"`
	}{}

	if err := json.Unmarshal(apiResp.Data, &v); err != nil {
		return promql.Vector{}, err
	}
	return d.decodeQueryResponse(v.Type, v.Result)
}

func (d jsonDecoder) decodeQueryResponse(valTyp model.ValueType, result json.RawMessage) (promql.Vector, error) {
	switch valTyp {
	case model.ValScalar:
		var sv model.Scalar
		if err := json.Unmarshal(result, &sv); err != nil {
			return nil, err
		}
		return d.scalarToPromQLVector(&sv), nil

	case model.ValVector:
		var vv model.Vector
		if err := json.Unmarshal(result, &vv); err != nil {
			return nil, err
		}
		return d.vectorToPromQLVector(vv), nil

	default:
		return nil, fmt.Errorf("rule result is not a vector or scalar: %q", valTyp)
	}
}

func (jsonDecoder) vectorToPromQLVector(vec model.Vector) promql.Vector {
	retVal := make(promql.Vector, 0, len(vec))
	for _, p := range vec {

		lbl := make(labels.Labels, 0, len(p.Metric))
		for ln, lv := range p.Metric {
			lbl = append(lbl, labels.Label{
				Name:  string(ln),
				Value: string(lv),
			})
		}

		retVal = append(retVal, promql.Sample{
			Metric: lbl,
			Point: promql.Point{
				V: float64(p.Value),
				T: int64(p.Timestamp),
			},
		})
	}
	return retVal
}

func (jsonDecoder) scalarToPromQLVector(sc *model.Scalar) promql.Vector {
	return promql.Vector{promql.Sample{
		Point: promql.Point{
			V: float64(sc.Value),
			T: int64(sc.Timestamp),
		},
		Metric: labels.Labels{},
	}}
}
