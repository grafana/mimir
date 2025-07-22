// SPDX-License-Identifier: AGPL-3.0-only
//go:build !stringlabels

package streamingpromql

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

func FixUpEmptyLabels(r *promql.Result) error {
	if r == nil || r.Value == nil {
		return nil
	}
	switch r.Value.Type() {
	case parser.ValueTypeMatrix:
		matrix, error := r.Matrix()
		if error != nil {
			return error
		}
		for i, sample := range matrix {
			if sample.Metric == nil {
				matrix[i].Metric = labels.Labels{}
			}
		}
	}
	return nil
}
