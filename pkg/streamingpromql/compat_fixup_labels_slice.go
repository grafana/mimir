//go:build !stringlabels

package streamingpromql

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

func FixUpEmptyLabels(r *promql.Result) {
	if r == nil || r.Value == nil {
		return
	}
	switch r.Value.Type() {
	case parser.ValueTypeMatrix:
		matrix, _ := r.Matrix()
		for i := 0; i < len(matrix); i++ {
			if matrix[i].Metric == nil {
				matrix[i].Metric = labels.Labels{}
			}
		}
	}
}
