// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

type mimirJSONCodec struct {
	v1.JSONCodec
}

func (m mimirJSONCodec) CanEncode(resp *v1.Response) bool {
	if resp.Data == nil {
		return true
	}

	data, isData := resp.Data.(*v1.QueryData)
	if !isData {
		return true
	}

	switch data.ResultType {
	case parser.ValueTypeVector:
		return m.canEncodeVector(data.Result.(promql.Vector))
	case parser.ValueTypeMatrix:
		return m.canEncodeMatrix(data.Result.(promql.Matrix))
	default:
		return true
	}
}

func (mimirJSONCodec) canEncodeVector(v promql.Vector) bool {
	for _, s := range v {
		if s.H != nil {
			return false
		}
	}

	return true
}

func (mimirJSONCodec) canEncodeMatrix(m promql.Matrix) bool {
	for _, series := range m {
		for _, point := range series.Points {
			if point.H != nil {
				return false
			}
		}
	}
	return true
}
