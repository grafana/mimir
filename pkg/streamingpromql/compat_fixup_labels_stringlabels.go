// SPDX-License-Identifier: AGPL-3.0-only
//go:build stringlabels

package streamingpromql

import (
	"github.com/prometheus/prometheus/promql"
)

func FixUpEmptyLabels(r *promql.Result) {
	return
}
