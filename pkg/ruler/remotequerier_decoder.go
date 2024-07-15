// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/mimirpb"
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
		return promql.Vector{}, fmt.Errorf("query execution failed with error: %s", apiResp.Error)
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

		b := labels.NewScratchBuilder(len(p.Metric))
		for ln, lv := range p.Metric {
			b.Add(string(ln), string(lv))
		}
		b.Sort()

		retVal = append(retVal, promql.Sample{
			Metric: b.Labels(),
			F:      float64(p.Value),
			T:      int64(p.Timestamp),
		})
	}
	return retVal
}

func (jsonDecoder) scalarToPromQLVector(sc *model.Scalar) promql.Vector {
	return promql.Vector{promql.Sample{
		F:      float64(sc.Value),
		T:      int64(sc.Timestamp),
		Metric: labels.Labels{},
	}}
}

type protobufDecoder struct{}

func (protobufDecoder) ContentType() string {
	return mimirpb.QueryResponseMimeType
}

func (d protobufDecoder) Decode(body []byte) (promql.Vector, error) {
	resp := mimirpb.QueryResponse{}
	if err := resp.Unmarshal(body); err != nil {
		return promql.Vector{}, err
	}

	if resp.Status == mimirpb.QueryResponse_ERROR {
		return promql.Vector{}, fmt.Errorf("query execution failed with error: %s", resp.Error)
	}

	switch data := resp.Data.(type) {
	case *mimirpb.QueryResponse_Scalar:
		return d.decodeScalar(data.Scalar), nil
	case *mimirpb.QueryResponse_Vector:
		return d.decodeVector(data.Vector)
	default:
		return promql.Vector{}, fmt.Errorf("rule result is not a vector or scalar: \"%s\"", d.dataTypeToHumanFriendlyName(resp))
	}
}

func (d protobufDecoder) decodeScalar(s *mimirpb.ScalarData) promql.Vector {
	return promql.Vector{promql.Sample{
		F:      s.Value,
		T:      s.TimestampMs,
		Metric: labels.Labels{},
	}}
}

func (d protobufDecoder) decodeVector(v *mimirpb.VectorData) (promql.Vector, error) {
	floatSampleCount := len(v.Samples)
	samples := make(promql.Vector, floatSampleCount+len(v.Histograms))

	for i, s := range v.Samples {
		m, err := d.metricToLabels(s.Metric)
		if err != nil {
			return nil, err
		}

		samples[i] = promql.Sample{
			Metric: m,
			F:      s.Value,
			T:      s.TimestampMs,
		}
	}

	for i, s := range v.Histograms {
		m, err := d.metricToLabels(s.Metric)
		if err != nil {
			return nil, err
		}

		samples[floatSampleCount+i] = promql.Sample{
			Metric: m,
			// We must not use the address of the loop variable as that's reused.
			H: (&(v.Histograms[i].Histogram)).ToPrometheusModel(),
			T: s.TimestampMs,
		}
	}

	return samples, nil
}

func (protobufDecoder) metricToLabels(metric []string) (labels.Labels, error) {
	if len(metric)%2 != 0 {
		return labels.EmptyLabels(), fmt.Errorf("metric is malformed, it contains an odd number of symbols: %d", len(metric))
	}

	labelCount := len(metric) / 2
	b := labels.NewScratchBuilder(labelCount)

	for i := 0; i < labelCount; i++ {
		b.Add(metric[2*i], metric[2*i+1])
	}

	return b.Labels(), nil
}

func (protobufDecoder) dataTypeToHumanFriendlyName(resp mimirpb.QueryResponse) string {
	switch resp.Data.(type) {
	case *mimirpb.QueryResponse_Scalar:
		return "scalar"
	case *mimirpb.QueryResponse_String_:
		return "string"
	case *mimirpb.QueryResponse_Vector:
		return "vector"
	case *mimirpb.QueryResponse_Matrix:
		return "matrix"
	default:
		return fmt.Sprintf("%T", resp.Data)
	}
}
