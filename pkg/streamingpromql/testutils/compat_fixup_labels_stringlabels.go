// SPDX-License-Identifier: AGPL-3.0-only
//go:build !slicelabels && !dedupelabels

package testutils

import (
	"github.com/prometheus/prometheus/promql"
)

// FixUpEmptyLabels does nothing in a stringlabels build
func FixUpEmptyLabels(r *promql.Result) error {
	// we have nothing todo here as in this build context we can not have nil labels
	return nil
}
