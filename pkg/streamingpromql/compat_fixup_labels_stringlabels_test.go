// SPDX-License-Identifier: AGPL-3.0-only
//go:build stringlabels

package streamingpromql

import (
	"github.com/prometheus/prometheus/promql"
)

func FixUpEmptyLabels(r *promql.Result) error {
	// we have nothing todo here as in this build context we can not have nil labels
	return nil
}
