// SPDX-License-Identifier: AGPL-3.0-only
//go:build slicelabels

package testutils

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// FixUpEmptyLabels will replace any sample.Metric which is nil with an empty labels.Labels instance
// Note that this functionality only runs when not in a stringlabels build
func FixUpEmptyLabels(r *promql.Result) error {
	if r == nil || r.Value == nil {
		return nil
	}
	switch r.Value.Type() {
	case parser.ValueTypeMatrix:
		matrix, err := r.Matrix()
		if err != nil {
			return err
		}
		for i, sample := range matrix {
			if sample.Metric == nil {
				matrix[i].Metric = labels.Labels{}
			}
		}
	case parser.ValueTypeVector:
		vector, err := r.Vector()
		if err != nil {
			return err
		}
		for i, sample := range vector {
			if sample.Metric == nil {
				vector[i].Metric = labels.Labels{}
			}
		}
	}
	return nil
}
