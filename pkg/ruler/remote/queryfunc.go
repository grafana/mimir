// SPDX-License-Identifier: AGPL-3.0-only

package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
)

// QueryFunc returns a rules.QueryFunc derived from a Querier instance.
func QueryFunc(q *Querier) rules.QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		valTyp, res, err := q.Query(ctx, qs, t)
		if err != nil {
			return nil, err
		}
		return decodeQueryResponse(valTyp, res)
	}
}

func decodeQueryResponse(valTyp model.ValueType, result json.RawMessage) (promql.Vector, error) {
	switch valTyp {
	case model.ValScalar:
		var sv model.Scalar
		if err := json.Unmarshal(result, &sv); err != nil {
			return nil, err
		}
		return scalarToPromQLVector(&sv), nil

	case model.ValVector:
		var vv model.Vector
		if err := json.Unmarshal(result, &vv); err != nil {
			return nil, err
		}
		return vectorToPromQLVector(vv), nil

	default:
		return nil, fmt.Errorf("rule result is not a vector or scalar: %q", valTyp)
	}
}

func vectorToPromQLVector(vec prommodel.Vector) promql.Vector {
	var retVal promql.Vector
	for _, p := range vec {
		var sm promql.Sample

		sm.V = float64(p.Value)
		sm.T = int64(p.Timestamp)

		var lbl labels.Labels
		for ln, lv := range p.Metric {
			lbl = append(lbl, labels.Label{Name: string(ln), Value: string(lv)})
		}
		sm.Metric = lbl

		retVal = append(retVal, sm)
	}
	return retVal
}

func scalarToPromQLVector(sc *prommodel.Scalar) promql.Vector {
	return promql.Vector{promql.Sample{
		Point: promql.Point{
			V: float64(sc.Value),
			T: int64(sc.Timestamp),
		},
		Metric: labels.Labels{},
	}}
}
