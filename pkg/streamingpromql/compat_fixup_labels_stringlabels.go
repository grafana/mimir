//go:build stringlabels

package streamingpromql

import (
	"github.com/prometheus/prometheus/promql"
)

func FixUpEmptyLabels(r *promql.Result) {
	return
}
