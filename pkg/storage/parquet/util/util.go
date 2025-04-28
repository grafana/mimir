// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus-community/parquet-common/blob/306c8486441da41d2a655fa29d0e83820437cf23/util/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package util

import "github.com/parquet-go/parquet-go"

func CloneRows(rows []parquet.Row) []parquet.Row {
	rr := make([]parquet.Row, len(rows))
	for i, row := range rows {
		rr[i] = row.Clone()
	}
	return rr
}
